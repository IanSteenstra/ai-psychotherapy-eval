document.addEventListener('DOMContentLoaded', () => {
    const therapistFilter = document.getElementById('pairingsTherapistFilter');
    const subtypeFilter = document.getElementById('pairingsSubtypeFilter');
    const stateFilter = document.getElementById('pairingsStateFilter');
    const applyButton = document.getElementById('applyPairingFilters');
    const resetButton = document.getElementById('resetPairingFilters');
    const tableBody = document.getElementById('pairingsTableBody');
    const counter = document.getElementById('pairingsCounter');

    const THERAPIST_DISPLAY_NAMES = {
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

    async function fetchJSON(url) {
        const response = await fetch(url, { credentials: 'same-origin' });
        if (!response.ok) {
            throw new Error(`Request failed with status ${response.status}`);
        }
        return response.json();
    }

    function populateSelect(selectElement, values, placeholder, labelsMap = null) {
        selectElement.innerHTML = '';
        const defaultOption = document.createElement('option');
        defaultOption.value = '';
        defaultOption.textContent = placeholder;
        selectElement.appendChild(defaultOption);

        values.forEach((value) => {
            const option = document.createElement('option');
            option.value = value;
            option.textContent = (labelsMap && labelsMap[value]) ? labelsMap[value] : value;
            selectElement.appendChild(option);
        });
    }

    async function loadFilters() {
        try {
            const data = await fetchJSON('/api/pairings/filters');
            populateSelect(therapistFilter, data.therapists || [], 'All Therapists', THERAPIST_DISPLAY_NAMES);
            populateSelect(subtypeFilter, data.subtypes || [], 'All Subtypes');
            populateSelect(stateFilter, data.states || [], 'All Stages');
        } catch (error) {
            console.error('Failed to load pairing filters', error);
        }
    }

    function buildQueryParams() {
        const params = new URLSearchParams();
        const therapist = therapistFilter.value;
        const subtype = subtypeFilter.value;
        const state = stateFilter.value;

        if (therapist) {
            params.append('therapist', therapist);
        }
        if (subtype) {
            params.append('subtype', subtype);
        }
        if (state) {
            params.append('state', state);
        }

        return params.toString();
    }

    function renderEmptyState(message) {
        tableBody.innerHTML = '';
        const row = document.createElement('tr');
        const cell = document.createElement('td');
        cell.colSpan = 5;
        cell.className = 'text-center text-muted py-4';
        cell.textContent = message;
        row.appendChild(cell);
        tableBody.appendChild(row);
    }

    function renderPairings(rows) {
        tableBody.innerHTML = '';
        rows.forEach((row) => {
            const tr = document.createElement('tr');

            const pairingCell = document.createElement('td');
            pairingCell.textContent = row.pairing_id ?? '';
            tr.appendChild(pairingCell);

            const therapistCell = document.createElement('td');
            therapistCell.textContent = row.therapist_label || row.therapist_id || '';
            tr.appendChild(therapistCell);

            const patientNameCell = document.createElement('td');
            patientNameCell.textContent = row.patient_name || '';
            tr.appendChild(patientNameCell);

            const subtypeCell = document.createElement('td');
            subtypeCell.textContent = row.subtype_name || '';
            tr.appendChild(subtypeCell);

            const stateCell = document.createElement('td');
            stateCell.textContent = row.state_of_change || '';
            tr.appendChild(stateCell);

            tableBody.appendChild(tr);
        });
    }

    async function loadPairings() {
        counter.textContent = 'Loading...';
        const params = buildQueryParams();
        const url = params ? `/api/pairings?${params}` : '/api/pairings';

        try {
            const rows = await fetchJSON(url);
            if (!rows || rows.length === 0) {
                renderEmptyState('No pairings match the current filters.');
                counter.textContent = '0 pairings';
                return;
            }

            renderPairings(rows);
            counter.textContent = `${rows.length} pairing${rows.length === 1 ? '' : 's'}`;
        } catch (error) {
            console.error('Failed to load pairings', error);
            renderEmptyState('An error occurred while loading pairings.');
            counter.textContent = 'Error loading pairings';
        }
    }

    applyButton.addEventListener('click', () => {
        loadPairings();
    });

    resetButton.addEventListener('click', () => {
        therapistFilter.value = '';
        subtypeFilter.value = '';
        stateFilter.value = '';
        loadPairings();
    });

    (async function init() {
        await loadFilters();
        await loadPairings();
    })();
});
