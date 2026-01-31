package migrator

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/DGarbs51/lcmigrate/internal/config"
	"github.com/DGarbs51/lcmigrate/internal/data"
	"github.com/DGarbs51/lcmigrate/internal/preflight"
	"github.com/DGarbs51/lcmigrate/internal/prompt"
	"github.com/DGarbs51/lcmigrate/internal/schema"
	"github.com/DGarbs51/lcmigrate/internal/ui"
)

const (
	DefaultBatchSize = 10000
	TotalStages      = 6
)

// Migrator handles the migration process
type Migrator struct {
	config     config.MigrationConfig
	sourceConn *sql.DB
	destConn   *sql.DB
	extractor  schema.Extractor
	applier    schema.Applier
	transferer data.Transferer

	// Migration results
	tables    []schema.TableSchema
	views     []schema.ViewDef
	sequences []schema.SequenceDef
	totalRows int64
}

// Run executes the complete migration workflow
func Run(dryRun bool) error {
	startTime := time.Now()

	// 1. Prompt for configuration
	cfg := prompt.PromptMigrationConfig(dryRun)

	// 2. Run pre-flight checks
	preflightResult, err := preflight.Run(cfg)
	if err != nil {
		return fmt.Errorf("pre-flight failed: %w", err)
	}

	if preflightResult.Aborted {
		ui.Info("Migration aborted by user")
		return nil
	}

	if !preflightResult.Passed {
		ui.Error("Pre-flight checks failed. Cannot proceed with migration.")
		return nil
	}

	// 3. Ask for final confirmation
	if !dryRun {
		fmt.Println()
		if !prompt.Confirm("Proceed with migration?") {
			ui.Info("Migration cancelled by user")
			return nil
		}
	}

	// 4. Create migrator
	m := &Migrator{
		config:     cfg,
		sourceConn: preflightResult.SourceConn,
		destConn:   preflightResult.DestConn,
		extractor:  schema.NewExtractor(cfg.Source.Engine),
		applier:    schema.NewApplier(cfg.Source.Engine),
		transferer: data.NewTransferer(cfg.Source.Engine),
	}

	// Ensure connections are closed when done
	defer func() {
		if m.sourceConn != nil {
			m.sourceConn.Close()
		}
	}()
	defer func() {
		if m.destConn != nil {
			m.destConn.Close()
		}
	}()

	// 5. Run migration stages
	if err := m.runMigration(); err != nil {
		return err
	}

	// 6. Print summary
	duration := time.Since(startTime)
	ui.Summary(len(m.tables), m.totalRows, duration)

	return nil
}

// runMigration executes all migration stages
func (m *Migrator) runMigration() error {
	// Stage 1: Schema Migration
	if err := m.migrateSchema(); err != nil {
		return err
	}

	// Stage 2: Data Migration
	if err := m.migrateData(); err != nil {
		return err
	}

	// Stage 3: Create Indexes and Constraints
	if err := m.createIndexesAndConstraints(); err != nil {
		return err
	}

	// Stage 4: Create Views
	if err := m.createViews(); err != nil {
		return err
	}

	// Stage 5: Migrate Sequences (PostgreSQL only)
	if err := m.migrateSequences(); err != nil {
		return err
	}

	// Stage 6: Finalization
	if err := m.finalize(); err != nil {
		return err
	}

	return nil
}

// migrateSchema extracts and creates table schemas (without indexes/FKs)
func (m *Migrator) migrateSchema() error {
	ui.Phase(1, TotalStages, "Migrating schema...")
	startTime := time.Now()

	// Extract tables from source
	tables, err := m.extractor.ExtractTables(m.sourceConn, m.config.Source.Database)
	if err != nil {
		ui.PhaseFailed(err)
		return fmt.Errorf("failed to extract schema: %w", err)
	}
	m.tables = tables

	if m.config.DryRun {
		ui.DryRun(fmt.Sprintf("Would create %d tables", len(tables)))
		for _, t := range tables {
			ui.DryRun(fmt.Sprintf("  CREATE TABLE %s", t.Name))
		}
		ui.PhaseDone(time.Since(startTime))
		return nil
	}

	// Create tables in destination
	for _, table := range tables {
		if err := m.applier.CreateTable(m.destConn, table); err != nil {
			ui.PhaseFailed(err)
			return err
		}
	}

	ui.PhaseDone(time.Since(startTime))
	return nil
}

// migrateData transfers data from source to destination
func (m *Migrator) migrateData() error {
	ui.Phase(2, TotalStages, "Migrating data...")
	startTime := time.Now()

	if m.config.DryRun {
		// Show what would be transferred
		for _, table := range m.tables {
			rows, _ := m.transferer.EstimateRows(m.sourceConn, table.Name)
			ui.DryRun(fmt.Sprintf("Would copy %s rows from %s", ui.FormatNumber(rows), table.Name))
			m.totalRows += rows
		}
		ui.PhaseDone(time.Since(startTime))
		return nil
	}

	// Disable foreign key checks
	if err := m.transferer.DisableForeignKeyChecks(m.destConn); err != nil {
		ui.PhaseFailed(err)
		return fmt.Errorf("failed to disable FK checks: %w", err)
	}

	fmt.Println() // newline for table progress

	// Transfer each table
	for _, table := range m.tables {
		totalRows, _ := m.transferer.EstimateRows(m.sourceConn, table.Name)

		stats, err := m.transferer.TransferTable(
			m.sourceConn, m.destConn, table, DefaultBatchSize, false,
			func(rows int64) {
				ui.TableProgress(table.Name, rows, totalRows)
			},
		)
		if err != nil {
			ui.PhaseFailed(err)
			return err
		}

		ui.TableDone(table.Name, stats.RowsCopied, stats.Duration)
		m.totalRows += stats.RowsCopied
	}

	ui.PhaseDone(time.Since(startTime))
	return nil
}

// createIndexesAndConstraints creates indexes and foreign keys
func (m *Migrator) createIndexesAndConstraints() error {
	ui.Phase(3, TotalStages, "Creating indexes and constraints...")
	startTime := time.Now()

	// Count total indexes and FKs
	var totalIndexes, totalFKs int
	for _, t := range m.tables {
		totalIndexes += len(t.Indexes)
		totalFKs += len(t.ForeignKeys)
	}

	if m.config.DryRun {
		ui.DryRun(fmt.Sprintf("Would create %d indexes and %d foreign keys", totalIndexes, totalFKs))
		ui.PhaseDone(time.Since(startTime))
		return nil
	}

	// Create indexes
	for _, table := range m.tables {
		for _, idx := range table.Indexes {
			if err := m.applier.CreateIndex(m.destConn, idx); err != nil {
				ui.PhaseFailed(err)
				return err
			}
		}
	}

	// Create foreign keys
	for _, table := range m.tables {
		for _, fk := range table.ForeignKeys {
			if err := m.applier.CreateForeignKey(m.destConn, fk); err != nil {
				ui.PhaseFailed(err)
				return err
			}
		}
	}

	ui.PhaseDone(time.Since(startTime))
	return nil
}

// createViews creates views in dependency order
func (m *Migrator) createViews() error {
	ui.Phase(4, TotalStages, "Creating views...")
	startTime := time.Now()

	views, err := m.extractor.ExtractViews(m.sourceConn, m.config.Source.Database)
	if err != nil {
		ui.PhaseFailed(err)
		return fmt.Errorf("failed to extract views: %w", err)
	}
	m.views = views

	if len(views) == 0 {
		ui.PhaseSkipped("no views")
		return nil
	}

	if m.config.DryRun {
		ui.DryRun(fmt.Sprintf("Would create %d views", len(views)))
		for _, v := range views {
			ui.DryRun(fmt.Sprintf("  CREATE VIEW %s", v.Name))
		}
		ui.PhaseDone(time.Since(startTime))
		return nil
	}

	for _, view := range views {
		if err := m.applier.CreateView(m.destConn, view); err != nil {
			ui.PhaseFailed(err)
			return err
		}
	}

	ui.PhaseDone(time.Since(startTime))
	return nil
}

// migrateSequences migrates sequences (PostgreSQL only)
func (m *Migrator) migrateSequences() error {
	ui.Phase(5, TotalStages, "Migrating sequences...")
	startTime := time.Now()

	if m.config.Source.Engine != "pgsql" {
		ui.PhaseSkipped("MySQL")
		return nil
	}

	sequences, err := m.extractor.ExtractSequences(m.sourceConn, m.config.Source.Database)
	if err != nil {
		ui.PhaseFailed(err)
		return fmt.Errorf("failed to extract sequences: %w", err)
	}
	m.sequences = sequences

	if len(sequences) == 0 {
		ui.PhaseSkipped("no sequences")
		return nil
	}

	if m.config.DryRun {
		ui.DryRun(fmt.Sprintf("Would set %d sequence values", len(sequences)))
		ui.PhaseDone(time.Since(startTime))
		return nil
	}

	// Set sequence values
	for _, seq := range sequences {
		if err := m.applier.SetSequenceValue(m.destConn, seq); err != nil {
			ui.PhaseFailed(err)
			return err
		}
	}

	ui.PhaseDone(time.Since(startTime))
	return nil
}

// finalize performs final verification
func (m *Migrator) finalize() error {
	ui.Phase(6, TotalStages, "Finalizing...")
	startTime := time.Now()

	if m.config.DryRun {
		ui.DryRun("Would verify row counts")
		ui.PhaseDone(time.Since(startTime))
		return nil
	}

	// Re-enable foreign key checks
	if err := m.transferer.EnableForeignKeyChecks(m.destConn); err != nil {
		ui.PhaseFailed(err)
		return fmt.Errorf("failed to enable FK checks: %w", err)
	}

	// Verify row counts
	for _, table := range m.tables {
		sourceRows, _ := m.transferer.EstimateRows(m.sourceConn, table.Name)
		destRows, _ := m.transferer.EstimateRows(m.destConn, table.Name)

		if sourceRows != destRows {
			err := fmt.Errorf("row count mismatch for %s: source=%d, dest=%d",
				table.Name, sourceRows, destRows)
			ui.PhaseFailed(err)
			return err
		}
	}

	ui.PhaseDone(time.Since(startTime))
	return nil
}
