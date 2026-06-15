export default function CatalogTableList({
  tables,
  selectedName,
  onSelect,
  filter,
  onFilterChange,
  listClassName = 'max-h-48',
}) {
  const filteredTables = tables?.filter(name =>
    name.toLowerCase().includes(filter.trim().toLowerCase()),
  ) ?? []

  if (!tables) return null

  if (tables.length === 0) {
    return <p className="mt-2 text-xs text-slate-400">No tables found in the catalog.</p>
  }

  return (
    <div className="mt-2 border border-edge rounded-lg overflow-hidden">
      <div className="px-3 py-2 border-b border-edge bg-surface-hover">
        <input
          type="text"
          value={filter}
          onChange={e => onFilterChange(e.target.value)}
          placeholder="Filter tables…"
          className="w-full border border-edge bg-edge rounded-md px-3 py-1.5 text-xs text-ink placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-accent/40 focus:border-accent transition"
        />
      </div>
      {filteredTables.length === 0 ? (
        <p className="px-3 py-2 text-xs text-slate-400">No tables match your filter.</p>
      ) : (
        <ul className={`overflow-y-auto divide-y divide-edge ${listClassName}`}>
          {filteredTables.map(name => (
            <li key={name}>
              <button
                type="button"
                onClick={() => onSelect(name)}
                className={`w-full text-left px-3 py-2 text-sm font-mono transition ${
                  selectedName === name
                    ? 'bg-accent-muted text-ink'
                    : 'text-slate-300 hover:bg-surface-hover hover:text-ink'
                }`}
              >
                {name}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
