(function() {
    const filterElements = {
        therapist: document.getElementById('therapistFilter'),
        subtype: document.getElementById('subtypeFilter'),
        state: document.getElementById('stateFilter'),
        session: document.getElementById('sessionFilter')
    };

    const filterButtons = {
        therapist: document.getElementById('therapistFilterBtn'),
        subtype: document.getElementById('subtypeFilterBtn'),
        state: document.getElementById('stateFilterBtn'),
        session: document.getElementById('sessionFilterBtn')
    };

    const defaultLabels = {
        therapist: 'All Therapists',
        subtype: 'All Subtypes',
        state: 'All Stages',
        session: 'All Sessions'
    };

    const selectedFilters = {
        therapist: [],
        subtype: [],
        state: [],
        session: []
    };

    const availableOptions = {
        therapist: [],
        subtype: [],
        state: [],
        session: []
    };

    const interventionsContainer = document.getElementById('interventionsContainer');
    const resultsCounter = document.getElementById('resultsCounter');
    const applyButton = document.getElementById('applyFilters');
    const resetButton = document.getElementById('resetFilters');
    const crisisSelect = document.getElementById('crisisFilter');
    let chainModalElement = null;
    let chainModalInstance = null;

    document.addEventListener('DOMContentLoaded', () => {
        fetchFilters();
        applyButton.addEventListener('click', loadInterventions);
        resetButton.addEventListener('click', resetFilters);
    });

    function fetchFilters() {
        fetch('/api/filters')
            .then(response => response.json())
            .then(data => {
                availableOptions.therapist = data.therapists || [];
                availableOptions.subtype = data.subtypes || [];
                availableOptions.state = data.states || [];
                availableOptions.session = (data.sessions || []).map(String);

                populateCheckboxFilter('therapist');
                populateCheckboxFilter('subtype');
                populateCheckboxFilter('state');
                populateCheckboxFilter('session');

                updateAllFilterLabels();
            })
            .catch(error => {
                console.error('Failed to load filter options', error);
                interventionsContainer.innerHTML = '<p class="text-danger mb-0">Unable to load filters. Please refresh the page.</p>';
            });
    }

    function populateCheckboxFilter(filterName) {
        const container = filterElements[filterName];
        if (!container) return;
        const options = availableOptions[filterName];

        container.innerHTML = '';

        const selectAllId = `${filterName}SelectAll`;
        const selectAllItem = document.createElement('li');
        selectAllItem.innerHTML = `
            <div class="form-check">
                <input class="form-check-input" type="checkbox" id="${selectAllId}">
                <label class="form-check-label" for="${selectAllId}"><strong>Select All</strong></label>
            </div>
        `;
        container.appendChild(selectAllItem);
        selectAllItem.addEventListener('click', event => event.stopPropagation());

        const selectAllCheckbox = selectAllItem.querySelector('input');
        selectAllCheckbox.checked = selectedFilters[filterName].length === options.length && options.length > 0;
        selectAllCheckbox.addEventListener('change', event => {
            if (event.target.checked) {
                selectedFilters[filterName] = [...options];
            } else {
                selectedFilters[filterName] = [];
            }
            container.querySelectorAll('input[type="checkbox"]').forEach(input => {
                if (input !== event.target) {
                    input.checked = event.target.checked;
                }
            });
            updateFilterButtonLabel(filterName);
        });

        options.forEach(optionValue => {
            const optionId = `${filterName}_${optionValue}`;
            const li = document.createElement('li');
            const labelText = filterName === 'therapist' ? getTherapistDisplayName(optionValue) : optionValue;
            li.innerHTML = `
                <div class="form-check">
                    <input class="form-check-input" type="checkbox" value="${optionValue}" id="${optionId}">
                    <label class="form-check-label" for="${optionId}">${escapeHtml(labelText)}</label>
                </div>
            `;
            container.appendChild(li);
            li.addEventListener('click', event => event.stopPropagation());

            const checkbox = li.querySelector('input');
            checkbox.checked = selectedFilters[filterName].includes(optionValue);
            checkbox.addEventListener('change', event => {
                const isChecked = event.target.checked;
                const values = selectedFilters[filterName];

                if (isChecked && !values.includes(optionValue)) {
                    values.push(optionValue);
                } else if (!isChecked) {
                    const idx = values.indexOf(optionValue);
                    if (idx >= 0) values.splice(idx, 1);
                }

                selectAllCheckbox.checked = values.length === options.length && options.length > 0;
                updateFilterButtonLabel(filterName);
            });
        });
    }

    function updateAllFilterLabels() {
        Object.keys(selectedFilters).forEach(updateFilterButtonLabel);
    }

    function updateFilterButtonLabel(filterName) {
        const button = filterButtons[filterName];
        if (!button) return;
        const values = selectedFilters[filterName];
        const options = availableOptions[filterName];
        let label = defaultLabels[filterName];

        if (values.length === options.length && options.length > 0) {
            label = 'All Selected';
        } else if (values.length === 1) {
            const value = values[0];
            label = filterName === 'therapist' ? getTherapistDisplayName(value) : value;
        } else if (values.length > 1) {
            label = `${values.length} selected`;
        }

        const target = button.querySelector('.filter-label');
        if (target) {
            target.textContent = label;
        } else {
            button.textContent = label;
        }
    }

    function resetFilters() {
        Object.keys(selectedFilters).forEach(key => {
            selectedFilters[key] = [];
        });
        crisisSelect.value = 'any';
        document.querySelectorAll('#intervention-filters input[type="checkbox"]').forEach(input => {
            input.checked = false;
        });
        updateAllFilterLabels();
        interventionsContainer.innerHTML = '<p class="text-muted mb-0">Use the filters above and click Apply to load matching sessions.</p>';
        resultsCounter.textContent = 'No sessions loaded';
    }

    function buildFilterParams() {
        const params = new URLSearchParams();
        Object.entries(selectedFilters).forEach(([filterName, values]) => {
            values.forEach(value => params.append(filterName, value));
        });
        const crisisValue = crisisSelect.value;
        if (crisisValue && crisisValue !== 'any') {
            params.set('crisis', crisisValue);
        }
        return params;
    }

    function loadInterventions() {
        const params = buildFilterParams();
        interventionsContainer.innerHTML = `
            <div class="text-center py-4">
                <div class="spinner-border text-primary" role="status"></div>
                <p class="text-muted mt-3 mb-0">Loading sessions...</p>
            </div>
        `;
    resultsCounter.textContent = 'Loading...';

        fetch(`/api/interventions?${params.toString()}`)
            .then(response => response.json())
            .then(data => {
                renderInterventions(Array.isArray(data) ? data : []);
            })
            .catch(error => {
                console.error('Failed to load interventions', error);
                interventionsContainer.innerHTML = '<p class="text-danger mb-0">Unable to load sessions. Please try again.</p>';
                resultsCounter.textContent = 'Error loading sessions';
            });
    }

    function renderInterventions(sessions) {
        if (!sessions.length) {
            interventionsContainer.innerHTML = '<p class="text-muted mb-0">No sessions match the selected filters.</p>';
            resultsCounter.textContent = '0 sessions';
            return;
        }

        let html = '<div class="accordion" id="interventionsAccordion">';
        sessions.forEach(session => {
            html += createAccordionItem(session);
        });
        html += '</div>';

        interventionsContainer.innerHTML = html;
        resultsCounter.textContent = `${sessions.length} session${sessions.length > 1 ? 's' : ''}`;

        document.querySelectorAll('.intervention-collapse').forEach(collapseEl => {
            collapseEl.addEventListener('show.bs.collapse', () => {
                const detailContainer = collapseEl.querySelector('.intervention-detail');
                if (!detailContainer || detailContainer.dataset.loaded === 'true') {
                    return;
                }
                detailContainer.innerHTML = '<div class="text-center py-3 text-muted">Loading session detail...</div>';
                fetchInterventionDetail(collapseEl.dataset.pairing, collapseEl.dataset.session, detailContainer);
            });
        });
    }

    function createAccordionItem(session) {
        const collapseId = `intervention-${session.pairing_id}-${session.session_id}`;
        const crisisBadge = session.crisis_occurred
            ? '<span class="badge bg-danger ms-2">Crisis</span>'
            : '<span class="badge bg-secondary ms-2">No Crisis</span>';
        const crisisSummary = Array.isArray(session.crisis_types) && session.crisis_types.length
            ? `<div class="mt-2 small text-danger">${session.crisis_types.map(escapeHtml).join(', ')}</div>`
            : '';
        const journalSummary = session.journal_summary ? truncateText(session.journal_summary, 220) : 'No journal summary available.';
        const therapistLabel = session.therapist_label || getTherapistDisplayName(session.therapist_id);

        return `
            <div class="accordion-item">
                <h2 class="accordion-header" id="heading-${collapseId}">
                    <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#${collapseId}" aria-expanded="false" aria-controls="${collapseId}">
                        <div class="w-100">
                            <div class="d-flex flex-wrap justify-content-between align-items-center">
                                <div>
                                    <span class="fw-semibold">Pairing ${escapeHtml(String(session.pairing_id))} &middot; Session ${escapeHtml(String(session.session_id))}</span>
                                    ${crisisBadge}
                                </div>
                                <div class="text-muted small">${escapeHtml(therapistLabel)}</div>
                            </div>
                            <div class="text-muted small">${escapeHtml(session.patient_name || 'Unknown patient')} &middot; ${escapeHtml(session.subtype_name || 'Subtype unknown')} &middot; ${escapeHtml(session.state_of_change || 'Stage unknown')}</div>
                            <p class="mb-0 mt-2 text-body">${escapeHtml(journalSummary)}</p>
                            ${crisisSummary}
                        </div>
                    </button>
                </h2>
                <div id="${collapseId}" class="accordion-collapse collapse intervention-collapse" aria-labelledby="heading-${collapseId}" data-bs-parent="#interventionsAccordion" data-pairing="${session.pairing_id}" data-session="${session.session_id}">
                    <div class="accordion-body intervention-detail" data-loaded="false">
                        <p class="text-muted mb-0">Expand to load transcript and full summary.</p>
                    </div>
                </div>
            </div>
        `;
    }

    function fetchInterventionDetail(pairingId, sessionId, container) {
        const url = `/api/interventions/detail?pairing_id=${encodeURIComponent(pairingId)}&session_id=${encodeURIComponent(sessionId)}`;
        fetch(url)
            .then(response => response.json())
            .then(data => {
                if (data && !data.error) {
                    renderInterventionDetail(container, data);
                } else {
                    container.innerHTML = '<p class="text-danger mb-0">Unable to load session detail.</p>';
                }
            })
            .catch(error => {
                console.error('Failed to load intervention detail', error);
                container.innerHTML = '<p class="text-danger mb-0">An error occurred while loading this session.</p>';
            });
    }

    function renderInterventionDetail(container, detail) {
        const crisisEvents = Array.isArray(detail.crisis_events) ? detail.crisis_events : [];
        const crisisTurns = new Map();
        crisisEvents.forEach(event => {
            if (!event) {
                return;
            }
            const turnNumber = Number(event.turn);
            if (!Number.isNaN(turnNumber)) {
                crisisTurns.set(turnNumber, event.classification);
            }
        });

        const riskIntensities = Array.isArray(detail.risk_intensities) ? [...detail.risk_intensities] : [];
        riskIntensities.sort((a, b) => (b.value || 0) - (a.value || 0));

        const adverseOutcomes = Array.isArray(detail.adverse_outcomes) ? detail.adverse_outcomes.map(item => ({
            id: item?.id,
            label: item?.label || item?.id || 'Adverse Outcome',
            occurred: Boolean(item?.occurred),
            attribution: item?.attribution || '',
            justification: item?.justification || ''
        })) : [];

        let html = '';

        html += '<div class="mb-4">';
        html += '<h6 class="fw-semibold">Transcript</h6>';
        if (!detail.transcript || !detail.transcript.length) {
            html += '<p class="text-muted mb-0">Transcript is not available for this session.</p>';
        } else {
            html += '<div class="transcript-block">';
            detail.transcript.forEach(line => {
                const turnNumber = Number(line.turn);
                const turnLabel = Number.isNaN(turnNumber) ? line.turn : turnNumber;
                const speaker = line.speaker || 'Unknown';
                const message = line.message || '';
                const classification = crisisTurns.get(turnNumber);
                const hasCrisis = crisisTurns.has(turnNumber);
                const speakerNormalized = speaker.toLowerCase();
                const hasChain = Array.isArray(line.chain_of_thought) && line.chain_of_thought.length > 0 && speakerNormalized === 'patient';
                const crisisClass = hasCrisis ? ' crisis-turn' : '';
                const badge = classification ? `<span class="badge bg-danger ms-2">${escapeHtml(classification)}</span>` : '';
                const chainMarkup = hasChain
                    ? `<button type="button" class="btn btn-outline-primary btn-sm chain-of-thought-btn" data-turn="${escapeHtml(String(turnLabel))}" data-chain="${encodeURIComponent(JSON.stringify(line.chain_of_thought))}"><i class="fas fa-lightbulb me-1"></i>Chain of Thought</button>`
                    : '';

                html += `
                    <div class="transcript-entry${crisisClass}">
                        <div class="small text-muted fw-semibold d-flex align-items-center justify-content-between">
                            <span>Turn ${escapeHtml(String(turnLabel))} &middot; ${escapeHtml(speaker)}${badge}</span>
                            ${chainMarkup}
                        </div>
                        <div>${formatMultiline(message)}</div>
                    </div>
                `;
            });
            html += '</div>';
        }
        html += '</div>';

        html += '<div class="mb-4">';
        html += '<h6 class="fw-semibold">After-Session Summary</h6>';
        html += `<p class="mb-2">${detail.journal_summary ? formatMultiline(detail.journal_summary) : '<span class="text-muted">No journal summary available for this session.</span>'}</p>`;
        if (detail.state_change_justification) {
            html += '<h6 class="fw-semibold">State Change Justification</h6>';
            html += `<p class="mb-2">${formatMultiline(detail.state_change_justification)}</p>`;
        }
        if (detail.crisis_types && detail.crisis_types.length) {
            html += '<div class="mb-2">';
            html += detail.crisis_types.map(type => `<span class="badge bg-danger-subtle text-danger-emphasis me-1">${escapeHtml(type)}</span>`).join('');
            html += '</div>';
        } else {
            html += '<p class="text-muted small mb-2">No crisis classifications recorded for this session.</p>';
        }
        if (adverseOutcomes.length) {
            html += '<div class="mt-3">';
            html += '<h6 class="fw-semibold">Adverse Outcomes</h6>';
            html += '<div class="list-group list-group-flush">';
            adverseOutcomes.forEach(outcome => {
                const occurred = outcome.occurred;
                const badgeClass = occurred ? 'bg-danger-subtle text-danger-emphasis' : 'bg-secondary-subtle text-secondary-emphasis';
                const badgeLabel = occurred ? 'Occurred' : 'Not Observed';
                const attribution = occurred && outcome.attribution ? `<div class="small text-muted">Attribution: ${escapeHtml(outcome.attribution)}</div>` : '';
                const justification = occurred && outcome.justification ? `<div class="small text-muted">Details: ${formatMultiline(outcome.justification)}</div>` : '';
                html += `
                    <div class="list-group-item px-0">
                        <div class="d-flex align-items-start justify-content-between">
                            <span class="fw-semibold${occurred ? ' text-danger' : ''}">${escapeHtml(outcome.label)}</span>
                            <span class="badge ${badgeClass}">${badgeLabel}</span>
                        </div>
                        ${attribution}
                        ${justification}
                    </div>
                `;
            });
            html += '</div>';
            html += '</div>';
        }
        html += '</div>';

        if (riskIntensities.length) {
            html += '<div class="mb-4">';
            html += '<h6 class="fw-semibold">Warning Sign Intensities</h6>';
            html += '<div class="row g-2">';
            riskIntensities.forEach(item => {
                html += `
                    <div class="col-sm-6 col-lg-4">
                        <div class="intensity-chip">
                            <span class="chip-label">${escapeHtml(item.label || item.id)}</span>
                            <span class="chip-value">${escapeHtml(String(item.value))}</span>
                        </div>
                    </div>
                `;
            });
            html += '</div>';
            html += '</div>';
        }

        container.innerHTML = html;
        container.dataset.loaded = 'true';
        attachChainOfThoughtHandlers(container);
    }

    function ensureChainModal() {
        if (!chainModalElement) {
            chainModalElement = document.createElement('div');
            chainModalElement.className = 'modal fade';
            chainModalElement.id = 'chainOfThoughtModal';
            chainModalElement.tabIndex = -1;
            chainModalElement.setAttribute('aria-hidden', 'true');
            chainModalElement.innerHTML = `
                <div class="modal-dialog modal-lg">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title">Patient Chain of Thought</h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                        </div>
                        <div class="modal-body">
                            <p class="text-muted mb-0">No chain of thought is available for this turn.</p>
                        </div>
                    </div>
                </div>
            `;
            document.body.appendChild(chainModalElement);
        }

        if (!chainModalInstance && typeof bootstrap !== 'undefined' && bootstrap.Modal) {
            chainModalInstance = bootstrap.Modal.getOrCreateInstance(chainModalElement);
        }

        return chainModalInstance ? { element: chainModalElement, instance: chainModalInstance } : null;
    }

    function attachChainOfThoughtHandlers(container) {
        const buttons = container.querySelectorAll('.chain-of-thought-btn');
        if (!buttons.length) {
            return;
        }

        buttons.forEach(button => {
            button.addEventListener('click', event => {
                event.preventDefault();
                const payload = button.dataset.chain ? decodeURIComponent(button.dataset.chain) : '';
                let chainItems = [];

                if (payload) {
                    try {
                        const parsed = JSON.parse(payload);
                        if (Array.isArray(parsed)) {
                            chainItems = parsed;
                        }
                    } catch (error) {
                        console.error('Failed to parse chain of thought payload', error);
                    }
                }

                if (!chainItems.length) {
                    return;
                }

                const modalResources = ensureChainModal();
                if (!modalResources) {
                    return;
                }

                const { element, instance } = modalResources;
                const modalTitle = element.querySelector('.modal-title');
                const modalBody = element.querySelector('.modal-body');

                if (modalTitle) {
                    const turnLabel = button.dataset.turn ? `Turn ${button.dataset.turn}` : 'Patient Chain of Thought';
                    modalTitle.textContent = turnLabel;
                }

                if (modalBody) {
                    const rows = chainItems.map(item => {
                        const label = escapeHtml(item.label || item.id || 'Detail');
                        const value = item.value === null || item.value === undefined ? '' : item.value;
                        const valueText = typeof value === 'number' ? String(value) : value;
                        return `
                            <tr>
                                <th scope="row" class="align-top text-nowrap">${label}</th>
                                <td>${formatMultiline(valueText)}</td>
                            </tr>
                        `;
                    }).join('');

                    modalBody.innerHTML = `
                        <div class="table-responsive">
                            <table class="table table-sm align-middle mb-0">
                                <tbody>${rows}</tbody>
                            </table>
                        </div>
                    `;
                }

                instance.show();
            });
        });
    }

    function truncateText(text, maxLength) {
        if (!text) return '';
        const normalized = String(text).replace(/\s+/g, ' ').trim();
        if (normalized.length <= maxLength) return normalized;
        return `${normalized.slice(0, maxLength - 3)}...`;
    }

    function escapeHtml(value) {
        if (value === null || value === undefined) {
            return '';
        }
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function formatMultiline(value) {
        if (value === null || value === undefined) {
            return '';
        }
        return escapeHtml(String(value)).replace(/\n/g, '<br>');
    }

    function getTherapistDisplayName(therapistId) {
        const mapping = {
            therapist_char: 'Character.AI',
            therapist_cai: 'Character.AI',
            therapist_chatgpt: 'ChatGPT',
            therapist_gpt_limited: 'ChatGPT',
            therapist_gpt_full: 'ChatGPT MI',
            therapist_gpt_4_mi: 'ChatGPT MI',
            therapist_gemini: 'Gemini',
            therapist_gemini_full: 'Gemini MI',
            therapist_gemini_mi: 'Gemini MI',
            therapist_gemini_harm: 'Harmful AI',
            therapist_safe: 'Safety Therapist',
            therapist_psych_material: 'NIAAA Booklet',
            therapist_psychological: 'Psychologist',
            therapist_niaaa: 'NIAAA Booklet'
        };
        return mapping[therapistId] || therapistId || 'Unknown Therapist';
    }
})();
