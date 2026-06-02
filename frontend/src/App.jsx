import { Navigate, Route, Routes } from 'react-router-dom'
import { MOCK_HOME_ROUTE, IS_MOCK } from './appConstants'
import HomePage from './pages/HomePage'
import GraphPage from './pages/GraphPage'
import MetadataPage from './pages/MetadataPage'
import TimelinePage from './pages/TimelinePage'
import FileTreePage from './pages/FileTreePage'
import TableLayout from './pages/TableLayout'
import NavBar from './components/NavBar'
import { TableSpecsProvider } from './context/TableSpecsContext'
import SnapshotSelectionPage from './pages/SnapshotSelectionPage'
import DocsPage from './pages/DocsPage'

function Layout({ children }) {
  return (
    <div className="min-h-screen bg-[#0d1117] flex flex-col">
      <NavBar />
      {children}
    </div>
  )
}

export default function App() {
  return (
    <TableSpecsProvider>
      <Routes>
        <Route
          path="/"
          element={
            IS_MOCK
              ? <Navigate to={MOCK_HOME_ROUTE} replace />
              : <Layout><HomePage /></Layout>
          }
        />
        <Route path="snapshots-selection" element={
          IS_MOCK
            ? <Navigate to={MOCK_HOME_ROUTE} replace />
            : <Layout><SnapshotSelectionPage /></Layout>
        } />
        <Route path="/docs" element={<Layout><DocsPage /></Layout>} />
        <Route
          path="/table"
          element={
            <Layout>
              <TableLayout />
            </Layout>
          }
        >
          <Route path="graph" element={<GraphPage />} />
          <Route path="metadata" element={<MetadataPage />} />
          <Route path="timeline" element={<TimelinePage />} />
          <Route path="filetree" element={<FileTreePage />} />
        </Route>
      </Routes>
    </TableSpecsProvider>
  )
}
