const { supabase } = require('./client');
const { getWorkspaces } = require('../powerbi/workspaces');
const { getDatasets, getTables } = require('../powerbi/datasets');

async function upsertWorkspaces(workspaces) {
  for (const ws of workspaces) {
    await supabase.from('workspaces').upsert(
      { id: ws.id, name: ws.name, type: ws.type, is_read_only: ws.isReadOnly },
      { onConflict: 'id' }
    );
  }
}

async function upsertDatasets(workspaceId, datasets) {
  for (const ds of datasets) {
    await supabase.from('datasets').upsert(
      {
        id: ds.id,
        workspace_id: workspaceId,
        name: ds.name,
        configured_by: ds.configuredBy,
        is_refreshable: ds.isRefreshable,
        created_date: ds.createdDate,
      },
      { onConflict: 'id' }
    );
  }
}

async function upsertTables(datasetId, tables) {
  for (const table of tables) {
    const { data: tableRow } = await supabase
      .from('tables')
      .upsert({ dataset_id: datasetId, name: table.name }, { onConflict: 'dataset_id,name' })
      .select('id')
      .single();

    if (tableRow && table.columns) {
      for (const col of table.columns) {
        await supabase.from('columns').upsert(
          {
            table_id: tableRow.id,
            name: col.name,
            data_type: col.dataType,
            column_type: col.columnType,
          },
          { onConflict: 'table_id,name' }
        );
      }
    }

    if (tableRow && table.measures) {
      for (const measure of table.measures) {
        await supabase.from('measures').upsert(
          {
            table_id: tableRow.id,
            name: measure.name,
            expression: measure.expression,
            description: measure.description,
          },
          { onConflict: 'table_id,name' }
        );
      }
    }
  }
}

async function syncAllMetadata() {
  const workspaces = await getWorkspaces();
  await upsertWorkspaces(workspaces);

  for (const ws of workspaces) {
    const datasets = await getDatasets(ws.id);
    await upsertDatasets(ws.id, datasets);

    for (const ds of datasets) {
      const tables = await getTables(ds.id);
      await upsertTables(ds.id, tables);
    }
  }

  return { workspaces: workspaces.length };
}

async function getDatasetSchema(datasetId) {
  const { data: tables } = await supabase
    .from('tables')
    .select(`
      id,
      name,
      columns ( name, data_type, column_type ),
      measures ( name, expression, description )
    `)
    .eq('dataset_id', datasetId);

  return tables || [];
}

module.exports = { syncAllMetadata, getDatasetSchema };
