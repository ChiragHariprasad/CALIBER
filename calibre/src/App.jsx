import React, { useState, useEffect } from 'react';

// Default initial state matching Scenario 1 (Synthetic Undercut)
const DEFAULT_STATE = {
  lap: 22,
  total_laps: 57,
  driver: "VER",
  position: 5,
  gap_ahead_s: 1.2,
  gap_behind_s: 3.4,
  driver_ahead: "RUS",
  driver_behind: "HAM",
  track_status: "GREEN",
  pit_lane_loss_s: 21.5,
  tyre: {
    compound: "MEDIUM",
    age_laps: 21,
    degradation_rate_s_per_lap: 0.114,
    current_pace_delta_s: 2.394,
    tyre_cliff_probability: 0.02,
    usable_laps_remaining: 5,
  },
  track: {
    grip_index: 0.88,
    grip_trend: "stable",
    track_temp_c: 36.5,
    air_temp_c: 25.0,
    evolution_delta_s: -0.33,
  },
  weather: {
    rain_probability_pct: 0.0,
    crossover_approaching: false,
    intermediate_window: false,
  },
  opponents: [
    { driver: "RUS", position: 4, gap_s: 1.2, gap_trend_s_per_lap: 0.0, compound: "MEDIUM", tyre_age_laps: 21, undercut_threat: true },
    { driver: "LEC", position: 3, gap_s: 4.5, gap_trend_s_per_lap: -0.1, compound: "HARD", tyre_age_laps: 14, undercut_threat: false },
    { driver: "HAM", position: 6, gap_s: 3.4, gap_trend_s_per_lap: 0.05, compound: "MEDIUM", tyre_age_laps: 20, undercut_threat: false },
  ]
};

const DEFAULT_DECISION = {
  decision: "PIT",
  urgency: "HIGH",
  target_lap: 22,
  target_compound: "HARD",
  window: [22, 24],
  expected_position: 1.0,
  podium_probability: 1.0,
  expected_points: 25.0,
  confidence: 0.88,
  why: [
    "Pit stop on Lap 22 for HARD compound.",
    "Executes undercut against RUS with fresh tyre out-lap pace advantage.",
    "Escapes medium compound degradation before imminent tyre cliff."
  ],
  counterfactual: {
    stay_out: {
      expected_position: 20.0,
      podium_probability: 0.0,
      expected_points: 0.0,
    }
  }
};

const DEFAULT_CANDIDATES = [
  { strategy_id: "STRAT-PIT-NOW-HARD", action: "PIT_NOW", pit_lap: 22, target_compound: "HARD", expected_position: 1.0, p10_position: 1.0, p50_position: 1.0, p90_position: 1.0, podium_probability: 1.0, expected_points: 25.0, why: ["Instant undercut advantage on Lap 22."] },
  { strategy_id: "STRAT-PIT-NEXT-HARD", action: "PIT_NEXT_LAP", pit_lap: 23, target_compound: "HARD", expected_position: 2.4, p10_position: 1.0, p50_position: 2.0, p90_position: 4.0, podium_probability: 0.85, expected_points: 18.0, why: ["One preparatory lap; risks car behind pitting first."] },
  { strategy_id: "STRAT-PIT-LATER-HARD", action: "PIT_LATER", pit_lap: 25, target_compound: "HARD", expected_position: 3.8, p10_position: 2.0, p50_position: 4.0, p90_position: 6.0, podium_probability: 0.60, expected_points: 12.0, why: ["Stretches stint to search for Safety Car."] },
  { strategy_id: "STRAT-STAY-OUT", action: "STAY_OUT", pit_lap: null, target_compound: "MEDIUM", expected_position: 20.0, p10_position: 18.0, p50_position: 20.0, p90_position: 20.0, podium_probability: 0.0, expected_points: 0.0, why: ["Severe pace collapse from tyre cliff beyond Lap 26."] },
];

const DEFAULT_LOSSES = [
  { corner: "T7", time_loss_s: 0.18, cause: "EARLY_BRAKING", confidence: 0.91, evidence: { reference_braking_point_m: 2345, actual_braking_point_m: 2338, delta_m: -7 }, recommendation: "Move braking point 7m later into Luffield while preserving apex rotation speed." },
  { corner: "T16", time_loss_s: 0.22, cause: "LATE_THROTTLE", confidence: 0.88, evidence: { reference_throttle_pickup_m: 5165, actual_throttle_pickup_m: 5176, delta_m: 11 }, recommendation: "Earlier throttle commitment on exit of Vale to maximize straight-line acceleration." },
];

export default function App() {
  const [activeTab, setActiveTab] = useState('command-centre');
  const [scenario, setScenario] = useState('synthetic-undercut');
  const [raceState, setRaceState] = useState(DEFAULT_STATE);
  const [decision, setDecision] = useState(DEFAULT_DECISION);
  const [candidates, setCandidates] = useState(DEFAULT_CANDIDATES);
  const [driverLosses, setDriverLosses] = useState(DEFAULT_LOSSES);
  const [replaySpeed, setReplaySpeed] = useState(10);
  const [isPlaying, setIsPlaying] = useState(true);

  // Live Multi-Point Feeds & Subsystem State
  const [sourcesHealth, setSourcesHealth] = useState(null);
  const [verdictsData, setVerdictsData] = useState(null);
  const [radioMessages, setRadioMessages] = useState([]);
  const [operatingMode, setOperatingMode] = useState({ mode: 'LIVE', feed_status: 'HEALTHY' });

  // Sync with API if running
  useEffect(() => {
    fetch('/api/v1/session')
      .then(res => res.json())
      .then(data => { if (data && data.lap) setRaceState(data); })
      .catch(() => {});

    fetch('/api/v1/strategy/recommendation')
      .then(res => res.json())
      .then(data => { if (data && data.decision) setDecision(data); })
      .catch(() => {});

    fetch('/api/v1/strategy/candidates')
      .then(res => res.json())
      .then(data => { if (Array.isArray(data) && data.length) setCandidates(data); })
      .catch(() => {});

    fetch('/api/v1/driver/losses')
      .then(res => res.json())
      .then(data => { if (Array.isArray(data) && data.length) setDriverLosses(data); })
      .catch(() => {});

    const pollLiveSubsystems = () => {
      fetch('/api/v1/live/sources')
        .then(res => res.json())
        .then(data => { if (data && data.sources) setSourcesHealth(data); })
        .catch(() => {});

      fetch('/api/v1/live/verdicts')
        .then(res => res.json())
        .then(data => { if (data && data.stats) setVerdictsData(data); })
        .catch(() => {});

      fetch('/api/v1/live/radio')
        .then(res => res.json())
        .then(data => { if (data && data.messages) setRadioMessages(data.messages); })
        .catch(() => {});

      fetch('/api/v1/live/mode')
        .then(res => res.json())
        .then(data => { if (data && data.mode) setOperatingMode(data); })
        .catch(() => {});
    };

    pollLiveSubsystems();
    const interval = setInterval(pollLiveSubsystems, 3000);
    return () => clearInterval(interval);
  }, [scenario]);

  const handleScenarioChange = (scName) => {
    setScenario(scName);
    fetch('/api/v1/replay/scenario', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ scenario: scName }),
    }).catch(() => {});

    // Local instant preview adjustments
    if (scName === 'safety-car') {
      setRaceState(prev => ({ ...prev, lap: 28, position: 3, track_status: "SAFETY_CAR", pit_lane_loss_s: 11.5 }));
      setDecision(prev => ({
        ...prev,
        decision: "PIT",
        urgency: "CRITICAL",
        target_lap: 28,
        why: ["Capitalize on 10.0s discounted pit loss under Safety Car conditions.", "Switch to Hard tyre for remainder of grand prix."]
      }));
    } else if (scName === 'tyre-cliff') {
      setRaceState(prev => ({
        ...prev,
        lap: 25,
        tyre: { ...prev.tyre, age_laps: 25, degradation_rate_s_per_lap: 0.38, tyre_cliff_probability: 0.85 }
      }));
      setDecision(prev => ({
        ...prev,
        decision: "PIT",
        urgency: "CRITICAL",
        target_lap: 25,
        why: ["Critical tyre cliff active (>85% probability).", "Immediate box required to prevent 3.5s/lap pace loss."]
      }));
    } else if (scName === 'weather-crossover') {
      setRaceState(prev => ({
        ...prev,
        weather: { rain_probability_pct: 78.0, crossover_approaching: true, intermediate_window: true }
      }));
      setDecision(prev => ({
        ...prev,
        decision: "PIT",
        target_compound: "INTERMEDIATE",
        why: ["Rain probability 78%: intermediate crossover window open.", "Slicks losing >4.5s/lap on damp track."]
      }));
    } else if (scName === 'false-driver-blame') {
      setRaceState(prev => ({ ...prev, track_status: "YELLOW" }));
      setDriverLosses([
        { corner: "T7", time_loss_s: 0.45, cause: "YELLOW_FLAG", confidence: 0.98, evidence: { track_status: "YELLOW" }, recommendation: "Maintain delta time; yellow flag restriction active." }
      ]);
    } else {
      setRaceState(DEFAULT_STATE);
      setDecision(DEFAULT_DECISION);
      setDriverLosses(DEFAULT_LOSSES);
    }
  };

  return (
    <div className="calibre-app">
      {/* Pit Wall Header */}
      <header className="pitwall-header">
        <div className="brand-section">
          <div className="f1-badge">F1 CALIBRE</div>
          <div>
            <div className="brand-title">RACE-ENGINEERING PIT WALL</div>
            <div className="brand-desc">Agentic Quantitative Strategy & Vehicle Dynamics Engine</div>
          </div>
        </div>

        <nav className="pitwall-nav">
          <button className={`pitwall-btn ${activeTab === 'command-centre' ? 'active' : ''}`} onClick={() => setActiveTab('command-centre')}>Command Centre</button>
          <button className={`pitwall-btn ${activeTab === 'strategy' ? 'active' : ''}`} onClick={() => setActiveTab('strategy')}>Strategy Optimizer</button>
          <button className={`pitwall-btn ${activeTab === 'track' ? 'active' : ''}`} onClick={() => setActiveTab('track')}>Track Map</button>
          <button className={`pitwall-btn ${activeTab === 'tyres' ? 'active' : ''}`} onClick={() => setActiveTab('tyres')}>Tyre Intelligence</button>
          <button className={`pitwall-btn ${activeTab === 'driver' ? 'active' : ''}`} onClick={() => setActiveTab('driver')}>Driver Losses</button>
          <button className={`pitwall-btn ${activeTab === 'integrity' ? 'active' : ''}`} onClick={() => setActiveTab('integrity')}>Feeds & Verdicts</button>
        </nav>

        <div className="scenario-selector">
          <span style={{ color: 'var(--text-muted)' }}>Scenario:</span>
          <select className="scenario-select" value={scenario} onChange={e => handleScenarioChange(e.target.value)}>
            <option value="synthetic-undercut">1. Synthetic Undercut</option>
            <option value="safety-car">2. Safety Car Neutralisation</option>
            <option value="tyre-cliff">3. Tyre Cliff Emergence</option>
            <option value="weather-crossover">4. Weather Crossover (Rain)</option>
            <option value="false-driver-blame">5. False Blame Attribution</option>
          </select>
        </div>
      </header>

      {/* Main Content */}
      <main className="pitwall-main">
        {/* Telemetry Ribbon */}
        <section className="telemetry-ribbon">
          <div className="telemetry-cell">
            <div className="cell-title">Position</div>
            <div className="cell-val" style={{ color: raceState.position <= 3 ? '#eab308' : '#fff' }}>P{raceState.position}</div>
          </div>
          <div className="telemetry-cell">
            <div className="cell-title">Lap Progress</div>
            <div className="cell-val">{raceState.lap} <span style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>/ {raceState.total_laps}</span></div>
          </div>
          <div className="telemetry-cell">
            <div className="cell-title">Gap to Ahead ({raceState.driver_ahead || 'LEAD'})</div>
            <div className="cell-val" style={{ color: (raceState.gap_ahead_s || 99) < 1.5 ? '#00d2be' : '#fff' }}>
              {raceState.gap_ahead_s ? `+${raceState.gap_ahead_s.toFixed(1)}s` : 'LEADER'}
            </div>
          </div>
          <div className="telemetry-cell">
            <div className="cell-title">Gap Behind ({raceState.driver_behind})</div>
            <div className="cell-val">+{raceState.gap_behind_s.toFixed(1)}s</div>
          </div>
          <div className="telemetry-cell">
            <div className="cell-title">Tyres (Age)</div>
            <div className="cell-val">
              <span className={`compound-badge ${raceState.tyre.compound}`}>{raceState.tyre.compound}</span>
              <span style={{ fontSize: '0.9rem', marginLeft: '0.4rem', color: 'var(--text-muted)' }}>({raceState.tyre.age_laps}L)</span>
            </div>
          </div>
          <div className="telemetry-cell">
            <div className="cell-title">Degradation Rate</div>
            <div className="cell-val" style={{ color: raceState.tyre.degradation_rate_s_per_lap > 0.2 ? '#ef4444' : '#facc15' }}>
              {raceState.tyre.degradation_rate_s_per_lap.toFixed(3)}s
            </div>
          </div>
          <div className="telemetry-cell">
            <div className="cell-title">Track Status</div>
            <div className="cell-val" style={{ color: raceState.track_status === 'GREEN' ? '#10b981' : '#eab308', fontSize: '1.1rem' }}>
              {raceState.track_status}
            </div>
          </div>
          <div className="telemetry-cell">
            <div className="cell-title">Pit Loss Delta</div>
            <div className="cell-val" style={{ color: raceState.pit_lane_loss_s < 15 ? '#00d2be' : '#fff' }}>
              {raceState.pit_lane_loss_s.toFixed(1)}s
            </div>
          </div>
        </section>

        {/* Tactical Recommendation Card */}
        <section className="recommendation-banner">
          <div className="rec-callout">
            <div className="rec-badge">AGENT STRATEGIC DIRECTIVE</div>
            <div className="rec-action" style={{ color: decision.decision === 'PIT' ? '#00d2be' : '#fff' }}>
              BOX: {decision.decision}
            </div>
            <div className="rec-sub">
              Target: <strong>Lap {decision.target_lap}</strong> for <strong>{decision.target_compound}</strong> tyres (Window: L{decision.window ? decision.window[0] : 22}–{decision.window ? decision.window[1] : 24})
            </div>
            <div className="rec-stats">
              <div className="rec-stat-box">
                <div className="box-lbl">Expected Finish</div>
                <div className="box-val" style={{ color: '#00d2be' }}>P{decision.expected_position.toFixed(1)}</div>
              </div>
              <div className="rec-stat-box">
                <div className="box-lbl">Podium Probability</div>
                <div className="box-val" style={{ color: '#eab308' }}>{(decision.podium_probability * 100).toFixed(0)}%</div>
              </div>
              <div className="rec-stat-box">
                <div className="box-lbl">Expected Points</div>
                <div className="box-val">{decision.expected_points.toFixed(1)}</div>
              </div>
            </div>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
            <h4 style={{ color: '#fff', fontSize: '0.9rem', marginBottom: '0.5rem', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
              Numerical Rationale & Counterfactuals:
            </h4>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem', fontSize: '0.85rem' }}>
              {decision.why && decision.why.map((r, i) => (
                <div key={i} style={{ color: 'var(--text-primary)', display: 'flex', gap: '0.5rem' }}>
                  <span style={{ color: '#00d2be' }}>✓</span>
                  <span>{r}</span>
                </div>
              ))}
              {decision.counterfactual && decision.counterfactual.stay_out && (
                <div style={{ marginTop: '0.5rem', padding: '0.5rem', background: 'rgba(239, 68, 68, 0.15)', border: '1px solid rgba(239, 68, 68, 0.4)', borderRadius: '4px', color: '#f87171', fontSize: '0.82rem' }}>
                  <strong>Alternative (STAY OUT):</strong> Expected finish drops to <strong>P{decision.counterfactual.stay_out.expected_position}</strong> due to severe tyre degradation penalty.
                </div>
              )}
            </div>
          </div>
        </section>

        {/* Tab 1: Command Centre */}
        {activeTab === 'command-centre' && (
          <div className="grid-2">
            <div className="pitwall-panel">
              <div className="panel-title-bar">
                <div className="panel-heading">⚔️ Competitor Battle & Undercut Gaps</div>
                <span className="compound-badge MEDIUM">Active Track</span>
              </div>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>
                <thead>
                  <tr style={{ color: 'var(--text-muted)', borderBottom: '1px solid var(--border)', textAlign: 'left' }}>
                    <th style={{ padding: '0.5rem' }}>Pos</th>
                    <th>Driver</th>
                    <th>Gap</th>
                    <th>Tyre</th>
                    <th>Age</th>
                    <th>Tactical Threat</th>
                  </tr>
                </thead>
                <tbody>
                  {raceState.opponents && raceState.opponents.map((opp, idx) => (
                    <tr key={idx} style={{ borderBottom: '1px solid var(--border)' }}>
                      <td style={{ padding: '0.6rem 0.5rem', fontWeight: 800 }}>P{opp.position}</td>
                      <td><strong style={{ color: '#fff' }}>{opp.driver}</strong></td>
                      <td>+{opp.gap_s.toFixed(1)}s</td>
                      <td><span className={`compound-badge ${opp.compound}`}>{opp.compound}</span></td>
                      <td>{opp.tyre_age_laps}L</td>
                      <td>
                        {opp.undercut_threat ? (
                          <span style={{ color: '#00d2be', fontWeight: 700 }}>⚡ Undercut Open</span>
                        ) : (
                          <span style={{ color: 'var(--text-muted)' }}>Stable</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="pitwall-panel">
              <div className="panel-title-bar">
                <div className="panel-heading">⚡ Driver Corner Telemetry Losses</div>
                <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>Reference Lap Delta</span>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                {driverLosses.map((l, i) => (
                  <div key={i} style={{ background: 'var(--bg-secondary)', padding: '0.75rem', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border)' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.35rem' }}>
                      <strong style={{ color: '#00d2be' }}>{l.corner} — {l.cause}</strong>
                      <span style={{ color: '#ef4444', fontWeight: 800 }}>-{l.time_loss_s.toFixed(2)}s</span>
                    </div>
                    <div style={{ fontSize: '0.82rem', color: 'var(--text-secondary)' }}>{l.recommendation}</div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* Tab 2: Strategy Optimizer */}
        {activeTab === 'strategy' && (
          <div className="pitwall-panel">
            <div className="panel-title-bar">
              <div className="panel-heading">🎯 Monte Carlo Strategy Candidates Comparison (100 Rollouts)</div>
              <span className="compound-badge HARD">Optimal Target</span>
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>
                <thead>
                  <tr style={{ color: 'var(--text-muted)', borderBottom: '1px solid var(--border)', textAlign: 'left' }}>
                    <th style={{ padding: '0.6rem' }}>Branch</th>
                    <th>Pit Lap</th>
                    <th>Compound</th>
                    <th>Expected Pos</th>
                    <th>P10 / P50 / P90</th>
                    <th>Podium Prob</th>
                    <th>Expected Pts</th>
                    <th>Strategic Rationale</th>
                  </tr>
                </thead>
                <tbody>
                  {candidates.map((c, i) => (
                    <tr key={i} style={{ borderBottom: '1px solid var(--border)', background: i === 0 ? 'rgba(0, 210, 190, 0.05)' : 'transparent' }}>
                      <td style={{ padding: '0.75rem 0.6rem' }}>
                        <strong style={{ color: i === 0 ? '#00d2be' : '#fff' }}>{c.action}</strong>
                        {i === 0 && <span style={{ marginLeft: '0.4rem', fontSize: '0.7rem', color: '#00d2be', border: '1px solid #00d2be', padding: '1px 4px', borderRadius: '3px' }}>OPTIMAL</span>}
                      </td>
                      <td>{c.pit_lap ? `Lap ${c.pit_lap}` : 'None'}</td>
                      <td><span className={`compound-badge ${c.target_compound}`}>{c.target_compound}</span></td>
                      <td><strong style={{ fontSize: '1.1rem', color: c.expected_position <= 3 ? '#eab308' : '#fff' }}>P{c.expected_position.toFixed(1)}</strong></td>
                      <td style={{ color: 'var(--text-muted)', fontFamily: 'monospace' }}>P{c.p10_position} / P{c.p50_position} / P{c.p90_position}</td>
                      <td><strong style={{ color: '#00d2be' }}>{(c.podium_probability * 100).toFixed(0)}%</strong></td>
                      <td><strong>{c.expected_points.toFixed(1)}</strong></td>
                      <td style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>{c.why ? c.why[0] : ''}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Tab 3: Track Map */}
        {activeTab === 'track' && (
          <div className="pitwall-panel">
            <div className="panel-title-bar">
              <div className="panel-heading">🏎️ Silverstone Circuit Geometry & Reference Trajectory</div>
              <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>5.891 km | 18 Turns | 2 DRS Zones</span>
            </div>
            <div style={{ textAlign: 'center', padding: '1.5rem 0' }}>
              <svg viewBox="0 0 700 320" style={{ width: '100%', maxHeight: '300px' }}>
                {/* Circuit Track Path */}
                <path
                  d="M 120 220 L 260 220 C 300 220, 320 180, 350 180 L 450 180 C 490 180, 520 120, 580 120 C 640 120, 660 60, 600 60 L 480 60 C 440 60, 420 100, 380 100 L 220 100 C 160 100, 140 140, 100 160 Z"
                  fill="none"
                  stroke="#2d3b55"
                  strokeWidth="14"
                  strokeLinejoin="round"
                />
                <path
                  d="M 120 220 L 260 220 C 300 220, 320 180, 350 180 L 450 180 C 490 180, 520 120, 580 120 C 640 120, 660 60, 600 60 L 480 60 C 440 60, 420 100, 380 100 L 220 100 C 160 100, 140 140, 100 160 Z"
                  fill="none"
                  stroke="#00d2be"
                  strokeWidth="3"
                  strokeDasharray="6"
                  strokeLinejoin="round"
                />
                {/* Corner Markers */}
                <circle cx="260" cy="220" r="7" fill="#ef4444" />
                <text x="260" y="245" fill="#fff" fontSize="12" fontWeight="bold" textAnchor="middle">T1 Abbey (295 kph)</text>

                <circle cx="350" cy="180" r="7" fill="#eab308" />
                <text x="350" y="205" fill="#fff" fontSize="12" fontWeight="bold" textAnchor="middle">T3 Village (105 kph)</text>

                <circle cx="450" cy="180" r="8" fill="#ef4444" />
                <text x="450" y="160" fill="#f87171" fontSize="12" fontWeight="bold" textAnchor="middle">T7 Luffield (-0.18s Early Brake)</text>

                <circle cx="600" cy="60" r="7" fill="#10b981" />
                <text x="600" y="45" fill="#fff" fontSize="12" fontWeight="bold" textAnchor="middle">T9 Copse (265 kph)</text>

                <circle cx="220" cy="100" r="8" fill="#ef4444" />
                <text x="220" y="80" fill="#f87171" fontSize="12" fontWeight="bold" textAnchor="middle">T16 Vale (-0.22s Late Throttle)</text>
              </svg>
            </div>
          </div>
        )}

        {/* Tab 4: Tyre Intelligence */}
        {activeTab === 'tyres' && (
          <div className="grid-2">
            <div className="pitwall-panel">
              <div className="panel-title-bar">
                <div className="panel-heading">🛞 Compound Degradation Curves</div>
                <span className="compound-badge MEDIUM">Active Set: Medium (21L)</span>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.85rem' }}>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', marginBottom: '0.3rem' }}>
                    <span style={{ color: '#ef4444', fontWeight: 700 }}>SOFT: 0.135 s/lap (Cliff at L16)</span>
                    <span>16 Laps Max</span>
                  </div>
                  <div style={{ height: '6px', background: 'var(--bg-secondary)', borderRadius: '3px', overflow: 'hidden' }}>
                    <div style={{ width: '100%', height: '100%', background: 'var(--soft-red)' }}></div>
                  </div>
                </div>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', marginBottom: '0.3rem' }}>
                    <span style={{ color: '#facc15', fontWeight: 700 }}>MEDIUM: 0.075 s/lap (Current: 21 / 26L)</span>
                    <strong style={{ color: '#00d2be' }}>Cliff approaching in 5 laps</strong>
                  </div>
                  <div style={{ height: '6px', background: 'var(--bg-secondary)', borderRadius: '3px', overflow: 'hidden' }}>
                    <div style={{ width: '80%', height: '100%', background: 'var(--medium-yellow)' }}></div>
                  </div>
                </div>
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', marginBottom: '0.3rem' }}>
                    <span style={{ color: '#f8fafc', fontWeight: 700 }}>HARD: 0.045 s/lap (Target for next stint)</span>
                    <span>36 Laps Life</span>
                  </div>
                  <div style={{ height: '6px', background: 'var(--bg-secondary)', borderRadius: '3px', overflow: 'hidden' }}>
                    <div style={{ width: '25%', height: '100%', background: 'var(--hard-white)' }}></div>
                  </div>
                </div>
              </div>
            </div>

            <div className="pitwall-panel">
              <div className="panel-title-bar">
                <div className="panel-heading">🌡️ Surface & Thermal Dynamics</div>
                <span className="compound-badge HARD">Grip 0.88</span>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem' }}>
                <div className="telemetry-cell">
                  <div className="cell-title">Track Temperature</div>
                  <div className="cell-val">36.5°C</div>
                </div>
                <div className="telemetry-cell">
                  <div className="cell-title">Air Temperature</div>
                  <div className="cell-val">25.0°C</div>
                </div>
                <div className="telemetry-cell">
                  <div className="cell-title">Track Grip Index</div>
                  <div className="cell-val" style={{ color: '#00d2be' }}>0.88</div>
                </div>
                <div className="telemetry-cell">
                  <div className="cell-title">Traction Slip Risk</div>
                  <div className="cell-val" style={{ color: '#facc15' }}>0.18</div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Tab 5: Driver Losses */}
        {activeTab === 'driver' && (
          <div className="pitwall-panel">
            <div className="panel-title-bar">
              <div className="panel-heading">⏱️ Granular Driver Corner Loss Attribution (§40)</div>
              <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Silverstone Q3 / Race Lap 22 Reference</span>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
              {driverLosses.map((l, i) => (
                <div key={i} style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', padding: '1rem' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
                      <span className="f1-badge" style={{ background: '#202b3f' }}>{l.corner}</span>
                      <strong style={{ color: '#fff', fontSize: '1rem' }}>{l.cause}</strong>
                      <span style={{ color: 'var(--text-muted)', fontSize: '0.8rem' }}>Confidence: {(l.confidence * 100).toFixed(0)}%</span>
                    </div>
                    <span style={{ fontSize: '1.2rem', fontWeight: 800, color: '#ef4444' }}>-{l.time_loss_s.toFixed(2)}s</span>
                  </div>
                  <div style={{ fontSize: '0.9rem', color: '#f8fafc', marginBottom: '0.5rem' }}>
                    {l.recommendation}
                  </div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', background: 'var(--bg-primary)', padding: '0.4rem 0.6rem', borderRadius: '4px', fontFamily: 'monospace' }}>
                    Evidence: {JSON.stringify(l.evidence)}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Tab 6: Feeds, Sensor Integrity & Driver Radio */}
        {activeTab === 'integrity' && (
          <div className="pitwall-panel">
            <div className="panel-title-bar">
              <div className="panel-heading">📡 Multi-Point Streaming, Sensor Integrity & Tactical Radio</div>
              <div style={{ display: 'flex', gap: '0.6rem', alignItems: 'center' }}>
                <span className="f1-badge" style={{ background: operatingMode.mode === 'LIVE' ? '#10b981' : '#eab308' }}>
                  MODE: {operatingMode.mode || 'LIVE'}
                </span>
                <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                  Feed: {operatingMode.feed_status || 'HEALTHY'}
                </span>
              </div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '1.25rem' }}>
              {/* Card 1: Multi-Point Sources Grid */}
              <div style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', padding: '1.25rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                  <strong style={{ color: '#fff', fontSize: '1.05rem' }}>🌐 Concurrent Data Feeds</strong>
                  <span style={{ fontSize: '0.8rem', color: '#00d2be' }}>
                    Throughput: {sourcesHealth?.throughput_eps || '32.4'} eps
                  </span>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                  {[
                    { id: 'f1_insights_live', name: 'F1 Insights Stream (SSE)', pri: 1, type: '4.0 Hz Live' },
                    { id: 'openf1_live', name: 'OpenF1 Telemetry API', pri: 2, type: '1.0 Hz Polled' },
                    { id: 'jolpica_f1', name: 'Jolpica-F1 Lap & Pit API', pri: 4, type: '0.5 Hz Historical/Live' },
                    { id: 'mock_feed', name: 'Scenario Telemetry Synthesizer', pri: 5, type: 'Fallback / Testing' },
                  ].map((src) => {
                    const health = sourcesHealth?.sources?.[src.id];
                    const status = health?.status || (src.pri === 1 ? 'CONSUMING' : 'CONNECTED');
                    const isConsuming = status === 'CONSUMING';
                    return (
                      <div key={src.id} style={{ background: 'var(--bg-primary)', padding: '0.75rem', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border)' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                          <span style={{ fontWeight: 600, color: '#f8fafc', fontSize: '0.9rem' }}>{src.name}</span>
                          <span style={{
                            fontSize: '0.7rem',
                            fontWeight: 700,
                            padding: '0.2rem 0.5rem',
                            borderRadius: '4px',
                            background: isConsuming ? 'rgba(16, 185, 129, 0.2)' : 'rgba(100, 116, 139, 0.2)',
                            color: isConsuming ? '#10b981' : '#94a3b8',
                          }}>
                            {status}
                          </span>
                        </div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.4rem' }}>
                          <span>Priority: P{src.pri} • {src.type}</span>
                          <span>Latency: {health?.latency_ms ? `${health.latency_ms}ms` : '<12ms'}</span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Card 2: Pit Lane Speed Monitor & CUSUM */}
              <div style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', padding: '1.25rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                  <strong style={{ color: '#fff', fontSize: '1.05rem' }}>⚖️ Pit Lane Speed & Verdicts</strong>
                  <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>FIA Limit: 60.0 km/h</span>
                </div>

                <div style={{ background: 'var(--bg-primary)', padding: '1rem', borderRadius: 'var(--radius-sm)', marginBottom: '1rem', border: '1px solid var(--border)' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div>
                      <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Recent Status</div>
                      <div style={{ fontSize: '1.4rem', fontWeight: 800, color: '#10b981' }}>
                        {verdictsData?.stats?.breaches > 0 ? 'INVESTIGATION' : 'CLEAR'}
                      </div>
                    </div>
                    <div style={{ textAlign: 'right' }}>
                      <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Evaluations / Alerts</div>
                      <div style={{ fontSize: '1.1rem', fontWeight: 700, color: '#fff' }}>
                        {verdictsData?.stats?.total_evaluated || 114} / {verdictsData?.stats?.integrity_alerts || 0}
                      </div>
                    </div>
                  </div>
                </div>

                <div style={{ fontSize: '0.85rem', color: '#cbd5e1', marginBottom: '0.5rem', fontWeight: 600 }}>
                  CUSUM Sensor Drift Protection (§38)
                </div>
                <div style={{ background: 'var(--bg-primary)', padding: '0.75rem', borderRadius: 'var(--radius-sm)', fontSize: '0.75rem', color: 'var(--text-muted)', border: '1px solid var(--border)' }}>
                  Active residual drift monitoring across pit entry timing loops. If on-car speed vs loop speed diverges by &gt; 4.0 km/h, potential breaches are automatically downgraded to <strong style={{ color: '#eab308' }}>DEFER</strong> to eliminate false driver penalties.
                </div>
              </div>

              {/* Card 3: Driver Radio Transmissions */}
              <div style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', padding: '1.25rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                  <strong style={{ color: '#fff', fontSize: '1.05rem' }}>📻 Driver Tactical Radio</strong>
                  <span style={{ fontSize: '0.8rem', color: '#e10600', fontWeight: 700 }}>● LIVE AIR</span>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem', maxHeight: '340px', overflowY: 'auto' }}>
                  {(radioMessages.length > 0 ? radioMessages : [
                    { sender: 'Race Engineer', category: 'BOX', priority: 'HIGH', message: 'Box at the end of this lap. Fitting Hard tyres. Confirm on pit button.', lap_number: 22 },
                    { sender: 'Performance Eng', category: 'BRAKE', priority: 'MEDIUM', message: 'Sector 2 pace note: Braking 7m too early into Turn 7. Apex rotation looks good.', lap_number: 21 },
                    { sender: 'Strategy Desk', category: 'TYRE', priority: 'MEDIUM', message: 'Degradation on Medium tyres is 0.11s/lap. Stint within window for undercut.', lap_number: 20 },
                  ]).map((msg, i) => (
                    <div key={i} style={{ background: 'var(--bg-primary)', borderLeft: msg.priority === 'CRITICAL' ? '3px solid #ef4444' : msg.priority === 'HIGH' ? '3px solid #eab308' : '3px solid #00d2be', padding: '0.75rem', borderRadius: 'var(--radius-sm)' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.75rem', marginBottom: '0.3rem' }}>
                        <span style={{ fontWeight: 700, color: '#f8fafc' }}>{msg.sender}</span>
                        <span style={{ color: 'var(--text-muted)' }}>Lap {msg.lap_number || raceState.lap}</span>
                      </div>
                      <div style={{ fontSize: '0.85rem', color: '#e2e8f0', fontStyle: 'italic' }}>
                        "{msg.message}"
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );

}
