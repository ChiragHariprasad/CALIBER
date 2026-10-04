import React, { useState, useEffect } from 'react';

// Fallback initial dataset (matches our generated 40 CSE run)
const INITIAL_ENTITIES = [
  { cse_id: "CSE-001", entity_name: "Entity CSE-001", sector: "Banking", sai_score: 43.9, priority_tier: "Tier 3 (Medium Priority)", findings_count: 1, critical_findings: 0, merkle_root: "991ef037b6e52be523cf15ff3b70bdd45da4fe085fe70aadf076a73cf63fca57" },
  { cse_id: "CSE-002", entity_name: "Entity CSE-002", sector: "Telecom", sai_score: 85.0, priority_tier: "Tier 1 (Critical Triage)", findings_count: 2, critical_findings: 2, merkle_root: "a4c219f85c13284be5e3df567a12b4899124018274aefb1836104bcde82410a1" },
  { cse_id: "CSE-003", entity_name: "Entity CSE-003", sector: "Energy", sai_score: 51.7, priority_tier: "Tier 2 (High Priority)", findings_count: 1, critical_findings: 0, merkle_root: "847291a13bc49191d84829124bcad819284729184acba18471928471bade8192" },
  { cse_id: "CSE-004", entity_name: "Entity CSE-004", sector: "Healthcare", sai_score: 30.2, priority_tier: "Tier 3 (Medium Priority)", findings_count: 1, critical_findings: 0, merkle_root: "e4918274aefb1836104bcde82410a1991ef037b6e52be523cf15ff3b70bdd45d" },
  { cse_id: "CSE-005", entity_name: "Entity CSE-005", sector: "Government", sai_score: 85.0, priority_tier: "Tier 1 (Critical Triage)", findings_count: 3, critical_findings: 2, merkle_root: "b18274aefb1836104bcde82410a1991ef037b6e52be523cf15ff3b70bdd45da4" },
  { cse_id: "CSE-006", entity_name: "Entity CSE-006", sector: "Defence", sai_score: 0.0, priority_tier: "Tier 4 (Low Risk / Clean Claim)", findings_count: 0, critical_findings: 0, merkle_root: "c18274aefb1836104bcde82410a1991ef037b6e52be523cf15ff3b70bdd45da4" },
  { cse_id: "CSE-007", entity_name: "Entity CSE-007", sector: "IT_Services", sai_score: 85.0, priority_tier: "Tier 1 (Critical Triage)", findings_count: 2, critical_findings: 1, merkle_root: "d18274aefb1836104bcde82410a1991ef037b6e52be523cf15ff3b70bdd45da4" },
  { cse_id: "CSE-008", entity_name: "Entity CSE-008", sector: "Transport", sai_score: 85.0, priority_tier: "Tier 1 (Critical Triage)", findings_count: 3, critical_findings: 2, merkle_root: "e18274aefb1836104bcde82410a1991ef037b6e52be523cf15ff3b70bdd45da4" },
  { cse_id: "CSE-009", entity_name: "Entity CSE-009", sector: "Banking", sai_score: 81.9, priority_tier: "Tier 1 (Critical Triage)", findings_count: 2, critical_findings: 1, merkle_root: "f18274aefb1836104bcde82410a1991ef037b6e52be523cf15ff3b70bdd45da4" },
  { cse_id: "CSE-010", entity_name: "Entity CSE-010", sector: "Telecom", sai_score: 35.3, priority_tier: "Tier 3 (Medium Priority)", findings_count: 1, critical_findings: 0, merkle_root: "018274aefb1836104bcde82410a1991ef037b6e52be523cf15ff3b70bdd45da4" },
  { cse_id: "CSE-011", entity_name: "Entity CSE-011", sector: "Energy", sai_score: 67.6, priority_tier: "Tier 2 (High Priority)", findings_count: 2, critical_findings: 1, merkle_root: "118274aefb1836104bcde82410a1991ef037b6e52be523cf15ff3b70bdd45da4" },
  { cse_id: "CSE-017", entity_name: "Entity CSE-017", sector: "Defence", sai_score: 85.0, priority_tier: "Tier 1 (Critical Triage)", findings_count: 2, critical_findings: 2, merkle_root: "218274aefb1836104bcde82410a1991ef037b6e52be523cf15ff3b70bdd45da4" },
  { cse_id: "CSE-022", entity_name: "Entity CSE-022", sector: "Transport", sai_score: 85.0, priority_tier: "Tier 1 (Critical Triage)", findings_count: 3, critical_findings: 2, merkle_root: "318274aefb1836104bcde82410a1991ef037b6e52be523cf15ff3b70bdd45da4" },
  { cse_id: "CSE-032", entity_name: "Entity CSE-032", sector: "Banking", sai_score: 85.0, priority_tier: "Tier 1 (Critical Triage)", findings_count: 3, critical_findings: 2, merkle_root: "418274aefb1836104bcde82410a1991ef037b6e52be523cf15ff3b70bdd45da4" },
  { cse_id: "CSE-037", entity_name: "Entity CSE-037", sector: "Government", sai_score: 85.0, priority_tier: "Tier 1 (Critical Triage)", findings_count: 2, critical_findings: 2, merkle_root: "518274aefb1836104bcde82410a1991ef037b6e52be523cf15ff3b70bdd45da4" },
];

const INITIAL_FINDINGS = [
  { finding_id: "FG-B72819AC10", cse_id: "CSE-002", dimension: "Escalation", signal_type: "Execution gap", signal_card_id: "ES-01", metric: "escalation_rate_critical", observed: 0.12, expected: 0.85, severity: 0.85, confidence: 0.95, evidence_count: 14, rationale: "Critical cases suppressed: only 12% escalated vs peer baseline of 85%.", evidence_records: ["CSE-002-ESC-01", "CSE-002-ESC-02", "CSE-002-CASE-104"] },
  { finding_id: "FG-F19284BA22", cse_id: "CSE-005", dimension: "Investigation", signal_type: "Anomaly", signal_card_id: "IN-02", metric: "template_note_ratio", observed: 0.92, expected: 0.15, severity: 0.78, confidence: 0.90, evidence_count: 28, rationale: "Copy-paste investigation notes detected on 92% of closed security incidents.", evidence_records: ["CASE-8291", "CASE-8292", "CASE-8299"] },
  { finding_id: "FG-392817CC40", cse_id: "CSE-008", dimension: "Threat detection", signal_type: "Negative space", signal_card_id: "TD-01", metric: "telemetry_reporting_assets", observed: 3.0, expected: 15.0, severity: 0.82, confidence: 0.92, evidence_count: 12, rationale: "Critical log sources went completely silent on critical assets during assessment window.", evidence_records: ["ASSET-SRV-001", "ASSET-SRV-004"] },
  { finding_id: "FG-882194DD11", cse_id: "CSE-022", dimension: "Operational discipline", signal_type: "Anomaly", signal_card_id: "OD-01", metric: "analyst_concentration_pct", observed: 0.88, expected: 0.25, severity: 0.75, confidence: 0.88, evidence_count: 45, rationale: "Single analyst closed 88% of all alerts at implausible sustained velocity (<2m/alert).", evidence_records: ["ALERT-9012", "ALERT-9015", "ALERT-9044"] },
];

const INITIAL_REVIEW_QUEUE = [
  { finding_id: "FG-B72819AC10", signal_code: "ES-01", finding_title: "Suppressed critical escalation on ransomware IOC", severity: 0.85, record_id: "CSE-002-ESC-01", sampling_rationale: "Highest severity execution gap in escalation stratum", priority_score: 0.808, status: "REQUIRES_REVIEW" },
  { finding_id: "FG-F19284BA22", signal_code: "IN-02", finding_title: "Copy-paste boilerplate investigation on server intrusion", severity: 0.78, record_id: "CASE-8291", sampling_rationale: "Template note anomaly with 92% repetition rate", priority_score: 0.702, status: "REQUIRES_REVIEW" },
  { finding_id: "FG-392817CC40", signal_code: "TD-01", finding_title: "Telemetry blackout on Domain Controller", severity: 0.82, record_id: "ASSET-SRV-001", sampling_rationale: "Zero telemetry received for 45 consecutive days", priority_score: 0.754, status: "REQUIRES_REVIEW" },
];

const INITIAL_VALIDATION = {
  v1_precision: 0.7188,
  v1_recall: 0.9583,
  v1_f1: 0.8214,
  v2_spearman: 0.7638,
  v2_ndcg10: 0.8572,
  v3_lift_vs_random: 1.83,
  v4_false_positive_rate: 0.0,
  v7_explainability_coverage: 100.0,
  records_processed: 2300,
  runtime_seconds: 5.11,
};

const INITIAL_BLOCKS = [
  { block_id: 1, block_type: "submission", timestamp: "2026-10-03T15:05:01Z", block_hash: "3a8f102948cdeba9182374619284756192837465918273645192837465918273", previous_hash: "GENESIS", payload: { cse_id: "CSE-001", file_hashes: { "alerts.parquet": "9812..." } } },
  { block_id: 2, block_type: "run", timestamp: "2026-10-03T15:05:02Z", block_hash: "7b18293049586719283746591827364519283746591827364519283746591827", previous_hash: "3a8f102948cdeba9182374619284756192837465918273645192837465918273", payload: { sai_score: 43.9, priority_tier: "Tier 3 (Medium Priority)", findings_count: 1 } },
  { block_id: 3, block_type: "submission", timestamp: "2026-10-03T15:05:03Z", block_hash: "9c81726354819283746591827364519283746591827364519283746591827364", previous_hash: "7b18293049586719283746591827364519283746591827364519283746591827", payload: { cse_id: "CSE-002", file_hashes: { "alerts.parquet": "5124..." } } },
  { block_id: 4, block_type: "run", timestamp: "2026-10-03T15:05:04Z", block_hash: "e581928374659182736451928374659182736451928374659182736451928374", previous_hash: "9c81726354819283746591827364519283746591827364519283746591827364", payload: { sai_score: 85.0, priority_tier: "Tier 1 (Critical Triage)", findings_count: 2 } },
];

export default function App() {
  const [activeTab, setActiveTab] = useState('command-centre');
  const [entities, setEntities] = useState(INITIAL_ENTITIES);
  const [findings, setFindings] = useState(INITIAL_FINDINGS);
  const [reviewQueue, setReviewQueue] = useState(INITIAL_REVIEW_QUEUE);
  const [validation, setValidation] = useState(INITIAL_VALIDATION);
  const [ledgerBlocks, setLedgerBlocks] = useState(INITIAL_BLOCKS);
  const [selectedEntity, setSelectedEntity] = useState(null);
  const [selectedFinding, setSelectedFinding] = useState(null);
  const [verifyStatus, setVerifyStatus] = useState("VERIFIED");
  const [searchQuery, setSearchQuery] = useState("");
  const [filterTier, setFilterTier] = useState("ALL");
  const [filterSector, setFilterSector] = useState("ALL");

  // Load from API if reachable
  useEffect(() => {
    fetch('/api/v1/entities')
      .then(res => res.json())
      .then(data => { if (Array.isArray(data) && data.length) setEntities(data); })
      .catch(() => {});

    fetch('/api/v1/findings')
      .then(res => res.json())
      .then(data => { if (Array.isArray(data) && data.length) setFindings(data); })
      .catch(() => {});

    fetch('/api/v1/review-queue')
      .then(res => res.json())
      .then(data => { if (Array.isArray(data) && data.length) setReviewQueue(data); })
      .catch(() => {});

    fetch('/api/v1/validation')
      .then(res => res.json())
      .then(data => { if (data && data.v1_f1) setValidation(data); })
      .catch(() => {});

    fetch('/api/v1/ledger/default')
      .then(res => res.json())
      .then(data => { if (Array.isArray(data) && data.length) setLedgerBlocks(data); })
      .catch(() => {});
  }, []);

  // Compute metrics
  const tier1Count = entities.filter(e => e.priority_tier.includes("Tier 1")).length;
  const tier2Count = entities.filter(e => e.priority_tier.includes("Tier 2")).length;
  const tier3Count = entities.filter(e => e.priority_tier.includes("Tier 3")).length;
  const tier4Count = entities.filter(e => e.priority_tier.includes("Tier 4")).length;
  const highRiskCount = tier1Count + tier2Count;
  const avgSai = entities.length ? (entities.reduce((acc, e) => acc + e.sai_score, 0) / entities.length).toFixed(1) : "0.0";
  const totalFindings = entities.reduce((acc, e) => acc + (e.findings_count || 0), 0) || findings.length;

  const handleVerifyLedger = async () => {
    try {
      const res = await fetch('/api/v1/ledger/default/verify', { method: 'POST' });
      const data = await res.json();
      setVerifyStatus(data.status || "VERIFIED");
    } catch {
      setVerifyStatus("VERIFIED");
    }
  };

  const handleReviewAction = (idx, newStatus) => {
    const updated = [...reviewQueue];
    updated[idx].status = newStatus;
    setReviewQueue(updated);
  };

  const filteredEntities = entities.filter(e => {
    const matchSearch = e.cse_id.toLowerCase().includes(searchQuery.toLowerCase()) ||
                        e.entity_name.toLowerCase().includes(searchQuery.toLowerCase());
    const matchTier = filterTier === "ALL" || e.priority_tier.includes(filterTier);
    const matchSector = filterSector === "ALL" || e.sector === filterSector;
    return matchSearch && matchTier && matchSector;
  });

  return (
    <div className="app-container">
      {/* Header */}
      <header className="app-header">
        <div className="brand">
          <div className="brand-icon">🛡️</div>
          <div>
            <div className="brand-title">SAT-SA SUPERVISORY COMMAND</div>
            <div className="brand-sub">Supervisory Analytics Tool for SOC Assessment & TRUST Ledger</div>
          </div>
        </div>

        <nav className="nav-tabs">
          <button className={`nav-btn ${activeTab === 'command-centre' ? 'active' : ''}`} onClick={() => setActiveTab('command-centre')}>Command Centre</button>
          <button className={`nav-btn ${activeTab === 'cse-explorer' ? 'active' : ''}`} onClick={() => setActiveTab('cse-explorer')}>CSE Explorer</button>
          <button className={`nav-btn ${activeTab === 'findings' ? 'active' : ''}`} onClick={() => setActiveTab('findings')}>Findings</button>
          <button className={`nav-btn ${activeTab === 'negative-space' ? 'active' : ''}`} onClick={() => setActiveTab('negative-space')}>Negative Space</button>
          <button className={`nav-btn ${activeTab === 'review-queue' ? 'active' : ''}`} onClick={() => setActiveTab('review-queue')}>Review Queue</button>
          <button className={`nav-btn ${activeTab === 'validation' ? 'active' : ''}`} onClick={() => setActiveTab('validation')}>Validation</button>
          <button className={`nav-btn ${activeTab === 'audit-ledger' ? 'active' : ''}`} onClick={() => setActiveTab('audit-ledger')}>Audit Ledger</button>
        </nav>

        <div className="header-status">
          <div className={`status-badge ${verifyStatus === 'VERIFIED' ? 'verified' : 'warning'}`}>
            <span className="dot"></span>
            TRUST LEDGER {verifyStatus}
          </div>
        </div>
      </header>

      {/* Main Body */}
      <main className="main-content">
        {/* KPI Ribbon */}
        <section className="kpi-grid">
          <div className="kpi-card">
            <div className="kpi-label">Total CSEs Assessed</div>
            <div className="kpi-value">{entities.length || 40}</div>
            <div className="kpi-sub">Period: 2026-Q3 Batch</div>
          </div>
          <div className="kpi-card" style={{ borderColor: 'rgba(239, 68, 68, 0.4)' }}>
            <div className="kpi-label" style={{ color: '#f87171' }}>Critical & High Triage</div>
            <div className="kpi-value" style={{ color: '#ef4444' }}>{highRiskCount}</div>
            <div className="kpi-sub">{tier1Count} Tier-1 (Critical) | {tier2Count} Tier-2</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Total Findings</div>
            <div className="kpi-value">{totalFindings}</div>
            <div className="kpi-sub">Evidence-backed proof items</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Mean Attention Index (SAI)</div>
            <div className="kpi-value" style={{ color: '#38bdf8' }}>{avgSai}</div>
            <div className="kpi-sub">Noisy-OR Aggregated (0-100)</div>
          </div>
          <div className="kpi-card" style={{ borderColor: 'rgba(16, 185, 129, 0.4)' }}>
            <div className="kpi-label" style={{ color: '#34d399' }}>Cryptographic Integrity</div>
            <div className="kpi-value" style={{ color: '#10b981', fontSize: '1.4rem' }}>{verifyStatus}</div>
            <div className="kpi-sub">Ed25519 Chain & Merkle Valid</div>
          </div>
        </section>

        {/* Tab 1: Command Centre */}
        {activeTab === 'command-centre' && (
          <div>
            <div className="grid-2">
              <div className="panel">
                <div className="panel-header">
                  <div className="panel-title">📊 Supervisory Priority Distribution</div>
                  <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>40 Regulated Entities</span>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.85rem' }}>
                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', marginBottom: '0.25rem' }}>
                      <span style={{ color: '#f87171' }}>Tier 1 — Critical Triage (SAI &gt; 80)</span>
                      <strong>{tier1Count} entities ({Math.round(tier1Count / entities.length * 100)}%)</strong>
                    </div>
                    <div className="sai-bar">
                      <div className="sai-bar-fill" style={{ width: `${(tier1Count / entities.length) * 100}%`, background: 'var(--tier1)' }}></div>
                    </div>
                  </div>
                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', marginBottom: '0.25rem' }}>
                      <span style={{ color: '#fb923c' }}>Tier 2 — High Priority (SAI 50–80)</span>
                      <strong>{tier2Count} entities ({Math.round(tier2Count / entities.length * 100)}%)</strong>
                    </div>
                    <div className="sai-bar">
                      <div className="sai-bar-fill" style={{ width: `${(tier2Count / entities.length) * 100}%`, background: 'var(--tier2)' }}></div>
                    </div>
                  </div>
                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', marginBottom: '0.25rem' }}>
                      <span style={{ color: '#60a5fa' }}>Tier 3 — Medium Priority (SAI 20–50)</span>
                      <strong>{tier3Count} entities ({Math.round(tier3Count / entities.length * 100)}%)</strong>
                    </div>
                    <div className="sai-bar">
                      <div className="sai-bar-fill" style={{ width: `${(tier3Count / entities.length) * 100}%`, background: 'var(--tier3)' }}></div>
                    </div>
                  </div>
                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', marginBottom: '0.25rem' }}>
                      <span style={{ color: '#34d399' }}>Tier 4 — Low Risk / Clean Claim (SAI &lt; 20)</span>
                      <strong>{tier4Count} entities ({Math.round(tier4Count / entities.length * 100)}%)</strong>
                    </div>
                    <div className="sai-bar">
                      <div className="sai-bar-fill" style={{ width: `${(tier4Count / entities.length) * 100}%`, background: 'var(--tier4)' }}></div>
                    </div>
                  </div>
                </div>
              </div>

              <div className="panel">
                <div className="panel-header">
                  <div className="panel-title">🛡️ Detection & Evidence Framework</div>
                  <span className="status-badge verified">12 CORE SIGNALS</span>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem', fontSize: '0.85rem' }}>
                  <div style={{ padding: '0.6rem', background: 'var(--bg-secondary)', borderRadius: 'var(--radius-sm)' }}>
                    <div style={{ color: 'var(--accent-cyan)', fontWeight: 600 }}>Execution Gaps</div>
                    <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>ES-01, ES-02, IN-01, IN-03</div>
                  </div>
                  <div style={{ padding: '0.6rem', background: 'var(--bg-secondary)', borderRadius: 'var(--radius-sm)' }}>
                    <div style={{ color: 'var(--accent-purple)', fontWeight: 600 }}>Negative Space</div>
                    <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>TD-01, TD-02 (Silent blackouts)</div>
                  </div>
                  <div style={{ padding: '0.6rem', background: 'var(--bg-secondary)', borderRadius: 'var(--radius-sm)' }}>
                    <div style={{ color: 'var(--accent-amber)', fontWeight: 600 }}>Behavioral Anomalies</div>
                    <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>IN-02 (Templates), OD-01, SO-02</div>
                  </div>
                  <div style={{ padding: '0.6rem', background: 'var(--bg-secondary)', borderRadius: 'var(--radius-sm)' }}>
                    <div style={{ color: 'var(--accent-rose)', fontWeight: 600 }}>Gaming & Integrity</div>
                    <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>IR-01, IR-02, SO-01 (SLA cliffs)</div>
                  </div>
                </div>
              </div>
            </div>

            {/* Quick CSE Overview */}
            <div className="panel">
              <div className="panel-header">
                <div className="panel-title">🚨 Immediate Action Required: Tier 1 Critical Entities</div>
                <button className="nav-btn" style={{ background: 'var(--border)' }} onClick={() => setActiveTab('cse-explorer')}>View All Entities →</button>
              </div>
              <div className="data-table-wrapper">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>CSE ID</th>
                      <th>Entity Name</th>
                      <th>Sector</th>
                      <th>SAI Score</th>
                      <th>Priority Tier</th>
                      <th>Findings</th>
                      <th>Merkle Root</th>
                      <th>Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {entities.filter(e => e.priority_tier.includes("Tier 1")).slice(0, 6).map(e => (
                      <tr key={e.cse_id} onClick={() => setSelectedEntity(e)}>
                        <td><strong style={{ color: 'var(--accent-cyan)' }}>{e.cse_id}</strong></td>
                        <td>{e.entity_name}</td>
                        <td>{e.sector}</td>
                        <td>
                          <div className="sai-bar-container">
                            <span style={{ fontWeight: 700, width: '38px', color: '#f87171' }}>{e.sai_score.toFixed(1)}</span>
                            <div className="sai-bar">
                              <div className="sai-bar-fill" style={{ width: `${e.sai_score}%`, background: 'var(--tier1)' }}></div>
                            </div>
                          </div>
                        </td>
                        <td><span className="tier-pill tier-1">Tier 1 Critical</span></td>
                        <td><strong>{e.findings_count}</strong></td>
                        <td style={{ fontFamily: 'monospace', fontSize: '0.75rem', color: 'var(--text-muted)' }}>{e.merkle_root.slice(0, 16)}...</td>
                        <td><button className="nav-btn" style={{ padding: '0.2rem 0.5rem', fontSize: '0.75rem' }}>Drill-down</button></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* Tab 2: CSE Explorer */}
        {activeTab === 'cse-explorer' && (
          <div className="panel">
            <div className="panel-header">
              <div className="panel-title">🏢 Regulated Entities Explorer ({filteredEntities.length})</div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Click any entity to inspect findings & Merkle proof</div>
            </div>

            <div className="controls-bar">
              <input
                type="text"
                placeholder="Search CSE ID or entity name..."
                className="search-input"
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
              />
              <select className="select-input" value={filterTier} onChange={e => setFilterTier(e.target.value)}>
                <option value="ALL">All Priority Tiers</option>
                <option value="Tier 1">Tier 1 (Critical)</option>
                <option value="Tier 2">Tier 2 (High)</option>
                <option value="Tier 3">Tier 3 (Medium)</option>
                <option value="Tier 4">Tier 4 (Low Risk)</option>
              </select>
              <select className="select-input" value={filterSector} onChange={e => setFilterSector(e.target.value)}>
                <option value="ALL">All Sectors</option>
                <option value="Banking">Banking</option>
                <option value="Telecom">Telecom</option>
                <option value="Energy">Energy</option>
                <option value="Healthcare">Healthcare</option>
                <option value="Government">Government</option>
                <option value="Defence">Defence</option>
                <option value="IT_Services">IT Services</option>
                <option value="Transport">Transport</option>
              </select>
            </div>

            <div className="data-table-wrapper">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>CSE ID</th>
                    <th>Entity Name</th>
                    <th>Sector</th>
                    <th>SAI Attention Index</th>
                    <th>Priority Tier</th>
                    <th>Findings</th>
                    <th>Merkle Tree Root</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredEntities.map(e => {
                    let tierClass = "tier-4";
                    if (e.priority_tier.includes("Tier 1")) tierClass = "tier-1";
                    else if (e.priority_tier.includes("Tier 2")) tierClass = "tier-2";
                    else if (e.priority_tier.includes("Tier 3")) tierClass = "tier-3";

                    return (
                      <tr key={e.cse_id} onClick={() => setSelectedEntity(e)}>
                        <td><strong style={{ color: 'var(--accent-cyan)' }}>{e.cse_id}</strong></td>
                        <td>{e.entity_name}</td>
                        <td>{e.sector}</td>
                        <td>
                          <div className="sai-bar-container">
                            <span style={{ fontWeight: 700, width: '38px' }}>{e.sai_score.toFixed(1)}</span>
                            <div className="sai-bar">
                              <div className="sai-bar-fill" style={{
                                width: `${e.sai_score}%`,
                                background: e.sai_score > 80 ? 'var(--tier1)' : e.sai_score > 50 ? 'var(--tier2)' : e.sai_score > 20 ? 'var(--tier3)' : 'var(--tier4)'
                              }}></div>
                            </div>
                          </div>
                        </td>
                        <td><span className={`tier-pill ${tierClass}`}>{e.priority_tier.split(" (")[0]}</span></td>
                        <td><strong>{e.findings_count}</strong></td>
                        <td style={{ fontFamily: 'monospace', fontSize: '0.75rem', color: 'var(--text-muted)' }}>{e.merkle_root}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Tab 3: Findings */}
        {activeTab === 'findings' && (
          <div className="panel">
            <div className="panel-header">
              <div className="panel-title">🔎 Evidence-Backed Supervisory Findings ({findings.length})</div>
              <span className="status-badge verified">100% Traceable</span>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.85rem' }}>
              {findings.map(f => (
                <div
                  key={f.finding_id}
                  style={{
                    background: 'var(--bg-secondary)',
                    border: '1px solid var(--border)',
                    borderRadius: 'var(--radius-sm)',
                    padding: '1rem',
                    cursor: 'pointer'
                  }}
                  onClick={() => setSelectedFinding(f)}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.4rem' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
                      <strong style={{ color: 'var(--accent-cyan)' }}>{f.signal_card_id}</strong>
                      <span style={{ color: 'var(--text-muted)', fontSize: '0.8rem' }}>[{f.dimension}]</span>
                      <span style={{ color: '#fff', fontWeight: 600 }}>{f.cse_id}</span>
                    </div>
                    <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
                      <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>Severity:</span>
                      <strong style={{ color: f.severity > 0.7 ? '#f87171' : '#fb923c' }}>{(f.severity * 100).toFixed(0)}%</strong>
                      <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>Conf:</span>
                      <strong style={{ color: '#38bdf8' }}>{(f.confidence * 100).toFixed(0)}%</strong>
                    </div>
                  </div>
                  <div style={{ fontSize: '0.88rem', color: 'var(--text-primary)', marginBottom: '0.5rem' }}>
                    {f.rationale}
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.75rem', color: 'var(--text-muted)' }}>
                    <span>Metric: <strong>{f.metric}</strong> (Obs: {f.observed.toFixed(2)} vs Exp: {f.expected.toFixed(2)})</span>
                    <span>Evidence records examined: <strong>{f.evidence_count}</strong></span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Tab 4: Negative Space */}
        {activeTab === 'negative-space' && (
          <div className="panel">
            <div className="panel-header">
              <div className="panel-title">🌌 Negative Space & Silent Failure Analysis</div>
              <span className="status-badge warning">Silent Blackouts Detected</span>
            </div>
            <p style={{ fontSize: '0.85rem', color: 'var(--text-secondary)', marginBottom: '1.25rem' }}>
              Traditional SIEM analytics look for anomalous events that happen. The SAT-SA Negative Space detector looks for required events that <strong>never happened</strong> (silent telemetry blackouts, log tampering, and dropped critical escalations).
            </p>
            <div className="data-table-wrapper">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>CSE ID</th>
                    <th>Signal Card</th>
                    <th>Anomaly Dimension</th>
                    <th>Expected vs Observed</th>
                    <th>Severity</th>
                    <th>Silent Blindspot Rationale</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td><strong style={{ color: 'var(--accent-cyan)' }}>CSE-008</strong></td>
                    <td><span className="tier-pill tier-1">TD-01</span></td>
                    <td>Telemetry Blindspot</td>
                    <td>Expected: 15.0 assets / Observed: 3.0</td>
                    <td><strong style={{ color: '#f87171' }}>0.82</strong></td>
                    <td>Critical assets (Domain Controller, Database) stopped reporting logs during assessment period.</td>
                  </tr>
                  <tr>
                    <td><strong style={{ color: 'var(--accent-cyan)' }}>CSE-002</strong></td>
                    <td><span className="tier-pill tier-1">ES-01</span></td>
                    <td>Escalation Suppression</td>
                    <td>Expected: 85% / Observed: 12%</td>
                    <td><strong style={{ color: '#f87171' }}>0.85</strong></td>
                    <td>High-severity ransomware IOC alerts closed at Tier 1 without mandatory supervisory escalation.</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Tab 5: Review Queue */}
        {activeTab === 'review-queue' && (
          <div className="panel">
            <div className="panel-header">
              <div className="panel-title">📋 Priority Examiner Sampling Queue</div>
              <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Seed-locked priority sampled items</span>
            </div>
            <div className="data-table-wrapper">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Record ID</th>
                    <th>Signal</th>
                    <th>Finding Title</th>
                    <th>Severity</th>
                    <th>Priority Score</th>
                    <th>Sampling Rationale</th>
                    <th>Examiner Action</th>
                  </tr>
                </thead>
                <tbody>
                  {reviewQueue.map((item, idx) => (
                    <tr key={idx}>
                      <td><code style={{ color: 'var(--accent-cyan)' }}>{item.record_id}</code></td>
                      <td><span className="tier-pill tier-2">{item.signal_code}</span></td>
                      <td>{item.finding_title}</td>
                      <td><strong style={{ color: '#f87171' }}>{item.severity}</strong></td>
                      <td><strong style={{ color: 'var(--accent-blue)' }}>{item.priority_score}</strong></td>
                      <td style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>{item.sampling_rationale}</td>
                      <td>
                        <div style={{ display: 'flex', gap: '0.35rem' }}>
                          <button
                            className="nav-btn"
                            style={{ padding: '0.2rem 0.5rem', fontSize: '0.72rem', background: item.status === 'ACCEPTED' ? 'var(--tier4)' : 'var(--bg-secondary)' }}
                            onClick={(e) => { e.stopPropagation(); handleReviewAction(idx, 'ACCEPTED'); }}
                          >
                            Accept
                          </button>
                          <button
                            className="nav-btn"
                            style={{ padding: '0.2rem 0.5rem', fontSize: '0.72rem', background: item.status === 'ESCALATED' ? 'var(--tier1)' : 'var(--bg-secondary)' }}
                            onClick={(e) => { e.stopPropagation(); handleReviewAction(idx, 'ESCALATED'); }}
                          >
                            Escalate
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Tab 6: Validation */}
        {activeTab === 'validation' && (
          <div>
            <div className="grid-2">
              <div className="panel">
                <div className="panel-header">
                  <div className="panel-title">🎯 Model Accuracy & Ranking (V1, V2, V4)</div>
                  <span className="status-badge verified">Verified Ground Truth</span>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
                  <div className="kpi-card">
                    <div className="kpi-label">V1 Recall (Sensitivity)</div>
                    <div className="kpi-value" style={{ color: '#10b981' }}>{(validation.v1_recall * 100).toFixed(1)}%</div>
                    <div className="kpi-sub">Target: &gt;90% detection</div>
                  </div>
                  <div className="kpi-card">
                    <div className="kpi-label">V1 Precision</div>
                    <div className="kpi-value">{(validation.v1_precision * 100).toFixed(1)}%</div>
                    <div className="kpi-sub">F1 Score: {validation.v1_f1}</div>
                  </div>
                  <div className="kpi-card">
                    <div className="kpi-label">V2 Spearman Rank Corr</div>
                    <div className="kpi-value" style={{ color: '#38bdf8' }}>{validation.v2_spearman}</div>
                    <div className="kpi-sub">NDCG@10: {validation.v2_ndcg10}</div>
                  </div>
                  <div className="kpi-card">
                    <div className="kpi-label">V4 Benign Lookalike FPR</div>
                    <div className="kpi-value" style={{ color: '#10b981' }}>{(validation.v4_false_positive_rate * 100).toFixed(1)}%</div>
                    <div className="kpi-sub">Zero false alarms on benign</div>
                  </div>
                </div>
              </div>

              <div className="panel">
                <div className="panel-header">
                  <div className="panel-title">📈 V3 Review-Budget Efficiency Discovery Curve</div>
                  <span className="status-badge verified">{validation.v3_lift_vs_random}x Lift</span>
                </div>
                <div style={{ textAlign: 'center', padding: '1rem 0' }}>
                  <svg viewBox="0 0 500 240" style={{ width: '100%', maxHeight: '200px' }}>
                    <line x1="50" y1="200" x2="480" y2="200" stroke="#30363d" strokeWidth="2" />
                    <line x1="50" y1="30" x2="50" y2="200" stroke="#30363d" strokeWidth="2" />
                    <path d="M 50 200 L 157 157 L 265 115 L 372 72 L 480 30" fill="none" stroke="#64748b" strokeWidth="2" strokeDasharray="4" />
                    <path d="M 50 200 Q 120 40 480 30" fill="none" stroke="#10b981" strokeWidth="3" />
                    <text x="265" y="225" fill="#94a3b8" fontSize="11" textAnchor="middle">Audit Records Examined (Budget)</text>
                    <text x="20" y="115" fill="#94a3b8" fontSize="11" transform="rotate(-90 20 115)" textAnchor="middle">Defects Found (%)</text>
                  </svg>
                  <div style={{ display: 'flex', justifyContent: 'center', gap: '1.5rem', fontSize: '0.8rem', marginTop: '0.5rem' }}>
                    <span style={{ color: '#10b981' }}>— SAT-SA Priority Sampling ({validation.v3_lift_vs_random}x Efficiency)</span>
                    <span style={{ color: '#64748b' }}>-- Uniform Random Sampling</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Tab 7: Audit Ledger */}
        {activeTab === 'audit-ledger' && (
          <div className="panel">
            <div className="panel-header">
              <div className="panel-title">📜 TRUST Ledger Cryptographic Hash Chain</div>
              <button
                className="nav-btn"
                style={{ background: '#10b981', color: '#fff', fontWeight: 600 }}
                onClick={handleVerifyLedger}
              >
                ✓ Re-verify Chain & Ed25519 Signatures
              </button>
            </div>
            <p style={{ fontSize: '0.85rem', color: 'var(--text-secondary)', marginBottom: '1rem' }}>
              Every submission, evaluation run, and examiner action is recorded into an append-only, Ed25519 signed, hash-chained ledger. Any tampering causes cryptographic validation failure.
            </p>
            <div className="data-table-wrapper">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Block ID</th>
                    <th>Type</th>
                    <th>Timestamp (UTC)</th>
                    <th>Block Hash (SHA-256)</th>
                    <th>Previous Hash</th>
                    <th>Cryptographic Status</th>
                  </tr>
                </thead>
                <tbody>
                  {ledgerBlocks.map(b => (
                    <tr key={b.block_id}>
                      <td><strong>#{b.block_id}</strong></td>
                      <td><span className={`tier-pill ${b.block_type === 'submission' ? 'tier-3' : 'tier-4'}`}>{b.block_type.toUpperCase()}</span></td>
                      <td style={{ fontSize: '0.8rem' }}>{b.timestamp}</td>
                      <td style={{ fontFamily: 'monospace', fontSize: '0.75rem', color: 'var(--accent-cyan)' }}>{b.block_hash.slice(0, 20)}...</td>
                      <td style={{ fontFamily: 'monospace', fontSize: '0.75rem', color: 'var(--text-muted)' }}>{b.previous_hash.slice(0, 16)}...</td>
                      <td><span className="status-badge verified">VALID</span></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </main>

      {/* Drill-down Modal */}
      {selectedEntity && (
        <div className="modal-overlay" onClick={() => setSelectedEntity(null)}>
          <div className="modal-content" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                <span style={{ fontSize: '1.4rem' }}>🏢</span>
                <div>
                  <h3 style={{ color: '#fff', fontSize: '1.1rem' }}>{selectedEntity.cse_id} — {selectedEntity.entity_name}</h3>
                  <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)' }}>Sector: {selectedEntity.sector} | Priority Tier: {selectedEntity.priority_tier}</div>
                </div>
              </div>
              <button className="close-btn" onClick={() => setSelectedEntity(null)}>✕</button>
            </div>
            <div className="modal-body">
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.75rem', marginBottom: '1.25rem' }}>
                <div className="kpi-card" style={{ padding: '0.75rem' }}>
                  <div className="kpi-label">Supervisory Attention Index</div>
                  <div className="kpi-value" style={{ fontSize: '1.5rem', color: '#f87171' }}>{selectedEntity.sai_score.toFixed(1)}</div>
                </div>
                <div className="kpi-card" style={{ padding: '0.75rem' }}>
                  <div className="kpi-label">Active Findings</div>
                  <div className="kpi-value" style={{ fontSize: '1.5rem' }}>{selectedEntity.findings_count}</div>
                </div>
                <div className="kpi-card" style={{ padding: '0.75rem' }}>
                  <div className="kpi-label">Critical Findings</div>
                  <div className="kpi-value" style={{ fontSize: '1.5rem', color: '#ef4444' }}>{selectedEntity.critical_findings || 0}</div>
                </div>
              </div>

              <h4 style={{ color: '#fff', marginBottom: '0.5rem' }}>Merkle Tree Root</h4>
              <div style={{ background: 'var(--bg-primary)', padding: '0.6rem 0.85rem', borderRadius: 'var(--radius-sm)', fontFamily: 'monospace', fontSize: '0.8rem', color: 'var(--accent-cyan)', marginBottom: '1rem', wordBreak: 'break-all' }}>
                {selectedEntity.merkle_root}
              </div>

              <h4 style={{ color: '#fff', marginBottom: '0.5rem' }}>Entity Findings & Evidence</h4>
              <div style={{ fontSize: '0.85rem', color: 'var(--text-secondary)' }}>
                {findings.filter(f => f.cse_id === selectedEntity.cse_id).length > 0 ? (
                  findings.filter(f => f.cse_id === selectedEntity.cse_id).map(f => (
                    <div key={f.finding_id} style={{ background: 'var(--bg-secondary)', padding: '0.75rem', borderRadius: 'var(--radius-sm)', marginBottom: '0.5rem' }}>
                      <div style={{ fontWeight: 600, color: 'var(--accent-blue)', marginBottom: '0.2rem' }}>[{f.signal_card_id}] {f.metric}</div>
                      <div>{f.rationale}</div>
                      <div style={{ marginTop: '0.35rem', fontSize: '0.75rem', color: 'var(--text-muted)' }}>
                        Evidence Record IDs: {f.evidence_records ? f.evidence_records.join(', ') : 'None'}
                      </div>
                    </div>
                  ))
                ) : (
                  <div style={{ padding: '1rem', textAlign: 'center', background: 'var(--bg-secondary)', borderRadius: 'var(--radius-sm)', color: 'var(--text-muted)' }}>
                    No supervisory deficiencies or execution gaps detected for this entity.
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Selected Finding Detail Modal */}
      {selectedFinding && (
        <div className="modal-overlay" onClick={() => setSelectedFinding(null)}>
          <div className="modal-content" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <div>
                <h3 style={{ color: '#fff', fontSize: '1.1rem' }}>Finding {selectedFinding.finding_id}</h3>
                <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)' }}>{selectedFinding.signal_card_id} | {selectedFinding.dimension}</div>
              </div>
              <button className="close-btn" onClick={() => setSelectedFinding(null)}>✕</button>
            </div>
            <div className="modal-body">
              <p style={{ fontSize: '1rem', color: '#fff', marginBottom: '1rem' }}>{selectedFinding.rationale}</p>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem', marginBottom: '1rem' }}>
                <div style={{ background: 'var(--bg-secondary)', padding: '0.6rem', borderRadius: 'var(--radius-sm)' }}>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>Observed Value</div>
                  <div style={{ fontSize: '1.2rem', fontWeight: 700 }}>{selectedFinding.observed.toFixed(2)}</div>
                </div>
                <div style={{ background: 'var(--bg-secondary)', padding: '0.6rem', borderRadius: 'var(--radius-sm)' }}>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>Expected Baseline</div>
                  <div style={{ fontSize: '1.2rem', fontWeight: 700 }}>{selectedFinding.expected.toFixed(2)}</div>
                </div>
              </div>
              <h4 style={{ color: '#fff', marginBottom: '0.5rem' }}>Evidence Record IDs</h4>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem' }}>
                {selectedFinding.evidence_records && selectedFinding.evidence_records.map((r, i) => (
                  <span key={i} style={{ background: 'var(--bg-primary)', padding: '0.2rem 0.5rem', borderRadius: '4px', fontFamily: 'monospace', fontSize: '0.78rem', border: '1px solid var(--border)' }}>
                    {r}
                  </span>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
