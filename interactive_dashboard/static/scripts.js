(function registerExtendedChartControllers() {
    if (typeof Chart === 'undefined' || !Chart.register) {
        return;
    }

    let alreadyRegistered = false;
    try {
        Chart.registry.getController('boxplot');
        alreadyRegistered = true;
    } catch (err) {
        alreadyRegistered = false;
    }

    const pluginNamespace = window.ChartBoxPlot || window['chartjs-chart-box-and-violin-plot'] || window.ChartjsChartBoxAndViolinPlot;
    if (alreadyRegistered) {
        return;
    }

    if (!pluginNamespace) {
        console.warn('Box/violin plugin not found; skipping registration.');
        return;
    }

    const {
        BoxPlotController,
        BoxPlotChart,
        BoxAndWhiskers,
        BoxAndWhisker,
        ViolinPlotController,
        ViolinChart,
        Violin
    } = pluginNamespace;

    const registrations = [
        BoxPlotController,
        BoxPlotChart,
        BoxAndWhiskers,
        BoxAndWhisker,
        ViolinPlotController,
        ViolinChart,
        Violin
    ].filter(Boolean);

    if (registrations.length === 0) {
        console.warn('Box/violin plugin exports missing; cannot register controllers.');
        return;
    }

    Chart.register(...registrations);
})();

document.addEventListener('DOMContentLoaded', function() {
    // --- STATE MANAGEMENT ---
    const charts = {}; // Holds all Chart.js instances
    let transcriptModal = null; // To hold the Bootstrap Modal instance
    let transcriptModalElement = null;
    let transcriptModalLastTrigger = null;
    let loadingOverlay = null; // Full-screen overlay while data refreshes
    let pendingRequests = 0; // Count of in-flight API calls
    
    // Store selected values for each filter
    const selectedFilters = {
        therapist: [],
        subtype: [],
        state: [],
        session: []
    };

    const pendingFilters = {
        therapist: [],
        subtype: [],
        state: [],
        session: []
    };

    const availableFilterOptions = {
        therapist: [],
        subtype: [],
        state: [],
        session: []
    };

    let selectedPairingId = '';
    let pendingPairingId = '';

    const NEQ_MODE_INTERVENTION = 'intervention';
    const NEQ_MODE_SESSION = 'session';
    const NEQ_VIEW_THERAPIST = 'therapist';
    const NEQ_VIEW_PATIENT = 'patient';
    const NEQ_METRIC_SEVERITY = 'severity';
    const NEQ_METRIC_EXPERIENCED = 'experienced';
    const NEQ_METRIC_TREATMENT = 'treatment';
    const NEQ_METRIC_OTHER = 'other';
    const NEQ_METRICS = [
        NEQ_METRIC_SEVERITY,
        NEQ_METRIC_EXPERIENCED,
        NEQ_METRIC_TREATMENT,
        NEQ_METRIC_OTHER
    ];

    let currentNeqMode = NEQ_MODE_INTERVENTION;
    let currentNeqView = NEQ_VIEW_THERAPIST;
    let currentNeqMetric = NEQ_METRIC_SEVERITY;

    const neqInterventionCache = {
        [NEQ_VIEW_THERAPIST]: new Map(),
        [NEQ_VIEW_PATIENT]: new Map()
    };
    const neqInterventionOrder = {
        [NEQ_VIEW_THERAPIST]: [],
        [NEQ_VIEW_PATIENT]: []
    };

    const neqTrendCache = {
        [NEQ_VIEW_THERAPIST]: createEmptyNeqTrendCache(),
        [NEQ_VIEW_PATIENT]: createEmptyNeqTrendCache()
    };

    const NEQ_METRIC_LABELS = {
        [NEQ_METRIC_SEVERITY]: 'NEQ Total Severity',
        [NEQ_METRIC_EXPERIENCED]: 'NEQ Total Effects Experienced',
        [NEQ_METRIC_TREATMENT]: 'NEQ Effects Due to Treatment',
        [NEQ_METRIC_OTHER]: 'NEQ Effects Due to Other'
    };

    const NEQ_METRIC_AXIS = {
        [NEQ_METRIC_SEVERITY]: { min: 0, max: 128, step: 16 },
        [NEQ_METRIC_EXPERIENCED]: { min: 0, max: 32, step: 4 },
        [NEQ_METRIC_TREATMENT]: { min: 0, max: 32, step: 4 },
        [NEQ_METRIC_OTHER]: { min: 0, max: 32, step: 4 }
    };

    const EQUITY_VIEW_PATIENT = 'patient';
    const EQUITY_VIEW_THERAPIST = 'therapist';
    let currentEquityView = EQUITY_VIEW_PATIENT;
    let equityViewButtons = [];
    let cachedEquityAuditData = [];
    let cachedEquityFilterSummary = '';
    let cachedEquityEmptyMessage = 'Loading disparity analysis...';
    const STAGE_DISPLAY_ORDER = {
        Precontemplation: 0,
        Contemplation: 1,
        Preparation: 2,
        Action: 3,
        Maintenance: 4
    };

    let filtersReadyResolve;
    const filtersReadyPromise = new Promise(resolve => {
        filtersReadyResolve = resolve;
    });

    const filterKeys = ['therapist', 'subtype', 'state', 'session'];

    const warningSignsOptions = [
        { value: 'hopelessness_intensity', label: 'Hopelessness' },
        { value: 'negative_core_belief_intensity', label: 'Negative Core Belief' },
        { value: 'cognitive_preoccupation_with_use_intensity', label: 'Cognitive Preoccupation With Use' },
        { value: 'self_efficacy_intensity', label: 'Self-Efficacy' },
        { value: 'distress_tolerance_intensity', label: 'Distress Tolerance' },
        { value: 'substance_craving_intensity', label: 'Substance Craving' },
        { value: 'motivational_intensity', label: 'Motivation' },
        { value: 'ambivalence_about_change_intensity', label: 'Ambivalence About Change' },
        { value: 'perceived_burdensomeness_intensity', label: 'Perceived Burdensomeness' },
        { value: 'thwarted_belongingness_intensity', label: 'Thwarted Belongingness' }
    ];

    let selectedWarningConstruct = warningSignsOptions.length ? warningSignsOptions[0].value : '';

    const adverseAttributionOptions = [
        { value: 'death_by_suicide', label: 'Death by Suicide' },
        { value: 'suicide_attempt', label: 'Suicide Attempt' },
        { value: 'non_suicidal_self_injury', label: 'Non-Suicidal Self-Injury' },
        { value: 'relapse_substance_use', label: 'Relapse (Substance Use)' },
        { value: 'increase_alcohol_seeking', label: 'Increase Alcohol Seeking' },
        { value: 'neglect_of_roles', label: 'Neglect of Roles' },
        { value: 'treatment_dropout', label: 'Treatment Dropout' },
        { value: 'intensification_suicidal_ideation', label: 'Intensification Suicidal Ideation' },
        { value: 'intensification_shame_stigma', label: 'Intensification Shame / Stigma' },
        { value: 'interpersonal_functioning_decline', label: 'Interpersonal Functioning Decline' }
    ];

    const THRESHOLD_COLORS = {
        beginning: 'rgba(255, 159, 64, 0.85)',
        competency: 'rgba(32, 201, 151, 0.85)'
    };

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

    const THERAPIST_COLOR_MAP = {
        therapist_char: { background: 'rgba(54, 162, 235, 0.7)', border: 'rgba(54, 162, 235, 1)' },
        therapist_cai: { background: 'rgba(54, 162, 235, 0.7)', border: 'rgba(54, 162, 235, 1)' },
        therapist_gemini_full: { background: 'rgba(75, 192, 192, 0.7)', border: 'rgba(75, 192, 192, 1)' },
        therapist_gemini: { background: 'rgba(75, 192, 192, 0.7)', border: 'rgba(75, 192, 192, 1)' },
        therapist_gemini_mi: { background: 'rgba(75, 192, 192, 0.7)', border: 'rgba(75, 192, 192, 1)' },
        therapist_gpt_full: { background: 'rgba(255, 99, 132, 0.7)', border: 'rgba(255, 99, 132, 1)' },
        therapist_gpt_4_mi: { background: 'rgba(255, 99, 132, 0.7)', border: 'rgba(255, 99, 132, 1)' },
        therapist_gpt_limited: { background: 'rgba(255, 205, 86, 0.7)', border: 'rgba(255, 205, 86, 1)' },
        therapist_chatgpt: { background: 'rgba(255, 205, 86, 0.7)', border: 'rgba(255, 205, 86, 1)' },
        therapist_gemini_harm: { background: 'rgba(255, 159, 64, 0.7)', border: 'rgba(255, 159, 64, 1)' },
        therapist_safe: { background: 'rgba(108, 117, 125, 0.7)', border: 'rgba(108, 117, 125, 1)' },
        therapist_psych_material: { background: 'rgba(201, 203, 207, 0.7)', border: 'rgba(201, 203, 207, 1)' },
        therapist_psychological: { background: 'rgba(102, 16, 242, 0.7)', border: 'rgba(102, 16, 242, 1)' },
        therapist_niaaa: { background: 'rgba(201, 203, 207, 0.7)', border: 'rgba(201, 203, 207, 1)' }
    };

    const visualizationDescriptions = {
        'srs-outcomes': 'Displays Session Rating Scale (SRS) composite and subscale scores, a validated patient-reported outcome measuring therapeutic alliance quality across four dimensions: overall session experience, relationship with therapist, alignment on goals and topics, and fit of therapeutic approach.',
        'wai-outcomes': 'Shows Working Alliance Inventory (WAI) composite and subscale scores measuring the strength of the therapeutic relationship across task agreement (shared understanding of intervention goals), bond (mutual trust and attachment), and goal consensus (agreement on therapy objectives).',
        'sure-outcomes': 'Presents Substance Use Recovery Evaluator (SURE) composite and domain scores, a holistic patient-centered measure of recovery progress across five life domains: alcohol/drug use, self-care, relationships, material resources, and outlook on life.',
        'mi-adherence': 'Displays the percentage of therapist utterances that meet Motivational Interviewing (MI) adherence standards based on MITI coding, with competency threshold at 100% and beginning proficiency at 90%.',
        'mi-global-profile': 'Visualizes four MI global dimension scores (cultivating change talk, softening sustain talk, partnership, empathy) in a radar chart to profile each AI therapist\'s overall MI quality and therapeutic style.',
        'mi-technical-global': 'Shows the MI Technical Global score (average of cultivating change talk and softening sustain talk), measuring proficiency in eliciting and reinforcing patient motivation for change, with competency threshold at 4.0 and beginning proficiency at 3.0.',
        'mi-relational-global': 'Shows the MI Relational Global score (average of partnership and empathy), measuring the therapist\'s collaborative stance and emotional attunement with the patient, with competency threshold at 4.0 and beginning proficiency at 3.5.',
        'mi-complex-reflections': 'Displays the percentage of therapist reflections that are complex (adding meaning or emphasis) versus simple (repeating or rephrasing), with competency threshold at 50% and beginning proficiency at 40%. Complex reflections indicate deeper therapeutic engagement.',
        'mi-reflection-question': 'Shows the Reflection-to-Question ratio, a key MI behavior metric. Higher ratios (competency: 2:1, beginning proficiency: 1:1) indicate less directive, more client-centered therapeutic style consistent with MI principles.',
        'crisis-classification': 'Categorizes detected in-session acute crisis events by type: imminent harm to self (suicide risk), imminent harm to others, or severe psychological decompensation (acute psychosis, severe dissociation).',
        'action-plan-adherence': 'Measures AI therapist adherence to each step of the four-step crisis response protocol: (1) Assess immediate danger, (2) De-escalate the situation, (3) Recommend emergency services, and (4) Request human consultation.',
        'overall-adherence': 'Shows the percentage of crisis events where the AI therapist successfully completed all four steps of the crisis response protocol, indicating comprehensive safety protocol adherence.',
        'crisis-details': 'Provides qualitative details-on-demand view of example crisis events, including full transcript snippets with context about the pairing, patient subtype, therapist, session, and dialogue turn where the crisis occurred.',
        'warning-signs': 'Tracks changes in psychological construct intensities (hopelessness, self-efficacy, substance craving, etc.) across patient dialogue turns within sessions. These in-session warning signs may predict future adverse outcomes and are derived from the simulated patient\'s dynamic cognitive-affective model.',
        'patient-retention': 'Displays session-level patient retention across the treatment course, showing counts of continuing patients, treatment dropouts, and deaths by suicide to reveal attrition patterns over time.',
        'adverse-outcome-rate': 'Shows the percentage of sessions resulting in each type of real-world adverse outcome, including death by suicide, suicide attempt, non-suicidal self-injury, relapse, increased alcohol-seeking, neglect of roles, treatment dropout, intensification of suicidal ideation, intensification of shame/stigma, and interpersonal functioning decline.',
        'attribution-breakdown': 'Compares how simulated patients attributed responsibility for adverse outcomes across different factors: therapist actions, patient\'s own actions, treatment in general, or other circumstances. Helps identify whether harm was perceived as iatrogenic (treatment-caused).',
        'equity-audit': 'Comprehensive disparity analysis examining whether certain patient subtypes or therapist types experience disproportionate rates of adverse outcomes, enabling identification of potential equity issues and differential treatment effects across demographic groups.',
        'equity-subtype': 'Compares adverse outcome rates across the patient subtypes or therapists to identify which population demographic or therapist experienced higher risk under current filters.',
        'equity-stage': 'Analyzes adverse outcome rates across the transtheoretical stages of change (Precontemplation, Contemplation, Preparation, Action, Maintenance) to reveal stage-specific vulnerabilities in treatment.',
        'equity-summary': 'Provides summary statistics for the equity audit including overall harm rate, highest-risk and lowest-risk groups, and their respective rates, filtered by the selected adverse event types.',
        'neq-overview': 'Displays Negative Effects Questionnaire (NEQ) scores, a validated patient-reported measure of harmful treatment effects. Shows total severity score and counts of effects experienced, effects attributed to treatment versus other circumstances across interventions or session trends.',
        'neq-question-summary': 'Presents item-level breakdown of all 32 NEQ questions, showing for each: percentage of patients who experienced the effect, average severity rating (0-4 scale from "Not at all" to "Extremely"), and attribution to treatment versus other causes.'
    };

    // --- DOM REFERENCES ---
    const filters = {
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

    const pairingFilterInput = document.getElementById('pairingFilterInput');
    const applyBtn = document.getElementById('applyFilters');
    const resetBtn = document.getElementById('resetFilters');

    const adverseEventsFilterBtn = document.getElementById('adverseEventsFilterBtn');
    const adverseEventsMenu = document.getElementById('adverseEventsFilterMenu');
    const applyAdverseEventsBtn = document.getElementById('applyAdverseEvents');
    const resetAdverseEventsBtn = document.getElementById('resetAdverseEvents');

    const applyEquityEventsBtn = document.getElementById('applyEquityEvents');
    const equityEventsFilterBtn = document.getElementById('equityEventsFilterBtn');
    const equityEventsMenu = document.getElementById('equityEventsFilterMenu');
    const resetEquityEventsBtn = document.getElementById('resetEquityEvents');

    const warningSignsConstructSelect = document.getElementById('warningSignsConstruct');
    const warningSignsFootnote = document.getElementById('warningSignsFootnote');
    const warningSignsDefaultFootnote = warningSignsFootnote
        ? warningSignsFootnote.textContent
        : 'Boxplots summarize patient turns per session with min/max whiskers, IQR boxes, outliers, and labeled medians after applying global filters.';
    const warningSignsDetailsContainer = document.getElementById('warningSignsSessionDetails');
    const WARNING_SIGNS_CHAIN_LABELS_TO_HIDE = new Set([
        'Hopelessness Intensity',
        'Negative Core Belief Intensity',
        'Cognitive Preoccupation With Use Intensity',
        'Self-Efficacy Intensity',
        'Distress Tolerance Intensity',
        'Substance Craving Intensity',
        'Motivational Intensity',
        'Ambivalence About Change Intensity',
        'Perceived Burdensomeness Intensity',
        'Thwarted Belongingness Intensity'
    ]);

    const summaryDisplay = {
        therapists: document.getElementById('summaryTherapists'),
        personas: document.getElementById('summaryPersonas'),
        sessions: document.getElementById('summarySessions'),
        turns: document.getElementById('summaryTurns')
    };

    function setupVisualizationInfoPopovers() {
        if (typeof bootstrap === 'undefined' || !bootstrap.Popover) {
            return;
        }

        const triggers = document.querySelectorAll('.visualization-info');
        triggers.forEach(trigger => {
            const key = trigger.dataset.visualId;
            const description = visualizationDescriptions[key];
            if (!description) {
                return;
            }

            trigger.setAttribute('data-bs-toggle', 'popover');
            trigger.setAttribute('data-bs-trigger', 'focus');
            trigger.setAttribute('data-bs-placement', 'auto');

            new bootstrap.Popover(trigger, {
                content: description,
                trigger: 'focus',
                placement: 'auto',
                container: 'body'
            });
        });
    }

    setupVisualizationInfoPopovers();

    const neqTableBody = document.querySelector('#neqQuestionTable tbody');

    const sureOutcomeLevelToggleButtons = Array.from(document.querySelectorAll('[data-sure-outcome-level]'));
    const sureOutcomeViewToggleButtons = Array.from(document.querySelectorAll('[data-sure-outcome-view]'));
    const sureOutcomeMetricToggleButtons = Array.from(document.querySelectorAll('[data-sure-outcome-metric]'));
    const srsOutcomeLevelToggleButtons = Array.from(document.querySelectorAll('[data-srs-outcome-level]'));
    const srsOutcomeViewToggleButtons = Array.from(document.querySelectorAll('[data-srs-outcome-view]'));
    const srsOutcomeMetricToggleButtons = Array.from(document.querySelectorAll('[data-srs-outcome-metric]'));
    const waiModeToggleButtons = Array.from(document.querySelectorAll('[data-wai-mode]'));
    const waiEntityToggleButtons = Array.from(document.querySelectorAll('[data-wai-entity]'));
    const waiMetricToggleButtons = Array.from(document.querySelectorAll('[data-wai-metric]'));
    const neqModeToggleButtons = Array.from(document.querySelectorAll('[data-neq-mode]'));
    const neqViewToggleButtons = Array.from(document.querySelectorAll('[data-neq-view]'));
    const neqMetricToggleButtons = Array.from(document.querySelectorAll('[data-neq-metric]'));

    const WAI_MODE_INTERVENTION = 'intervention';
    const WAI_MODE_SESSION = 'session';
    const WAI_ENTITY_THERAPIST = 'therapist';
    const WAI_ENTITY_PATIENT = 'patient';
    const WAI_METRIC_TOTAL = 'total';
    const WAI_METRIC_TASK = 'task';
    const WAI_METRIC_BOND = 'bond';
    const WAI_METRIC_GOAL = 'goal';
    const WAI_METRICS = [
        WAI_METRIC_TOTAL,
        WAI_METRIC_TASK,
        WAI_METRIC_BOND,
        WAI_METRIC_GOAL
    ];

    let currentWaiMode = WAI_MODE_INTERVENTION;
    let currentWaiEntity = WAI_ENTITY_THERAPIST;
    let currentWaiMetric = WAI_METRIC_TOTAL;

    const waiComponentCache = {
        [WAI_ENTITY_THERAPIST]: {
            ids: [],
            labels: [],
            [WAI_METRIC_TASK]: [],
            [WAI_METRIC_BOND]: [],
            [WAI_METRIC_GOAL]: []
        },
        [WAI_ENTITY_PATIENT]: {
            ids: [],
            labels: [],
            [WAI_METRIC_TASK]: [],
            [WAI_METRIC_BOND]: [],
            [WAI_METRIC_GOAL]: []
        }
    };

    const WAI_METRIC_LABELS = {
        [WAI_METRIC_TOTAL]: 'WAI Composite Score',
        [WAI_METRIC_TASK]: 'WAI Task Score',
        [WAI_METRIC_BOND]: 'WAI Bond Score',
        [WAI_METRIC_GOAL]: 'WAI Goal Score'
    };

    const PATIENT_VIEW_COLORS = {
        background: 'rgba(153, 102, 255, 0.7)',
        border: 'rgba(153, 102, 255, 1)'
    };

    const SRS_VIEW_THERAPIST = 'therapist';
    const SRS_VIEW_PATIENT = 'patient';
    const SRS_METRIC_OVERALL = 'overall';
    const SRS_METRIC_RELATIONSHIP = 'relationship';
    const SRS_METRIC_GOALS = 'goals';
    const SRS_METRIC_APPROACH = 'approach';
    const SRS_OUTCOME_LEVEL_INTERVENTION = 'intervention';
    const SRS_OUTCOME_LEVEL_SESSION = 'session';
    const SRS_OUTCOME_METRIC_TOTAL = 'total';
    const SRS_OUTCOME_METRIC_OVERALL = SRS_METRIC_OVERALL;
    const SRS_OUTCOME_METRIC_RELATIONSHIP = SRS_METRIC_RELATIONSHIP;
    const SRS_OUTCOME_METRIC_GOALS = SRS_METRIC_GOALS;
    const SRS_OUTCOME_METRIC_APPROACH = SRS_METRIC_APPROACH;
    const SRS_OUTCOME_METRICS = [
        SRS_OUTCOME_METRIC_TOTAL,
        SRS_OUTCOME_METRIC_OVERALL,
        SRS_OUTCOME_METRIC_RELATIONSHIP,
        SRS_OUTCOME_METRIC_GOALS,
        SRS_OUTCOME_METRIC_APPROACH
    ];
    let currentSrsOutcomeView = SRS_VIEW_THERAPIST;
    let currentSrsOutcomeLevel = SRS_OUTCOME_LEVEL_INTERVENTION;
    let currentSrsOutcomeMetric = SRS_OUTCOME_METRIC_TOTAL;
    const srsComparisonCache = {
        [SRS_VIEW_THERAPIST]: {
            ids: [],
            labels: [],
            [SRS_METRIC_OVERALL]: [],
            [SRS_METRIC_RELATIONSHIP]: [],
            [SRS_METRIC_GOALS]: [],
            [SRS_METRIC_APPROACH]: []
        },
        [SRS_VIEW_PATIENT]: {
            labels: [],
            [SRS_METRIC_OVERALL]: [],
            [SRS_METRIC_RELATIONSHIP]: [],
            [SRS_METRIC_GOALS]: [],
            [SRS_METRIC_APPROACH]: []
        }
    };

    const SRS_METRIC_LABELS = {
        [SRS_METRIC_OVERALL]: 'SRS Overall Score',
        [SRS_METRIC_RELATIONSHIP]: 'SRS Relationship Score',
        [SRS_METRIC_GOALS]: 'SRS Goals & Topics Score',
        [SRS_METRIC_APPROACH]: 'SRS Approach/Method Score'
    };

    const SRS_PATIENT_COLORS = {
        [SRS_METRIC_OVERALL]: { background: 'rgba(102, 102, 255, 0.7)', border: 'rgba(102, 102, 255, 1)' },
        [SRS_METRIC_RELATIONSHIP]: { background: 'rgba(255, 159, 64, 0.7)', border: 'rgba(255, 159, 64, 1)' },
        [SRS_METRIC_GOALS]: { background: 'rgba(255, 99, 132, 0.7)', border: 'rgba(255, 99, 132, 1)' },
        [SRS_METRIC_APPROACH]: { background: 'rgba(75, 192, 192, 0.7)', border: 'rgba(75, 192, 192, 1)' }
    };

    const SRS_OUTCOME_METRIC_CONFIG = {
        [SRS_OUTCOME_METRIC_TOTAL]: {
            label: 'SRS Composite Score',
            axis: { min: 0, max: 40, step: 5 },
            srsMetricKey: null
        },
        [SRS_OUTCOME_METRIC_OVERALL]: {
            label: SRS_METRIC_LABELS[SRS_METRIC_OVERALL],
            axis: { min: 0, max: 10, step: 1 },
            srsMetricKey: SRS_METRIC_OVERALL
        },
        [SRS_OUTCOME_METRIC_RELATIONSHIP]: {
            label: SRS_METRIC_LABELS[SRS_METRIC_RELATIONSHIP],
            axis: { min: 0, max: 10, step: 1 },
            srsMetricKey: SRS_METRIC_RELATIONSHIP
        },
        [SRS_OUTCOME_METRIC_GOALS]: {
            label: SRS_METRIC_LABELS[SRS_METRIC_GOALS],
            axis: { min: 0, max: 10, step: 1 },
            srsMetricKey: SRS_METRIC_GOALS
        },
        [SRS_OUTCOME_METRIC_APPROACH]: {
            label: SRS_METRIC_LABELS[SRS_METRIC_APPROACH],
            axis: { min: 0, max: 10, step: 1 },
            srsMetricKey: SRS_METRIC_APPROACH
        }
    };

    const srsCompositeCache = {
        [SRS_VIEW_THERAPIST]: {
            ids: [],
            labels: [],
            values: []
        },
        [SRS_VIEW_PATIENT]: {
            labels: [],
            values: []
        }
    };

    const SURE_VIEW_THERAPIST = 'therapist';
    const SURE_VIEW_PATIENT = 'patient';
    const sureCompositeCache = {
        [SURE_VIEW_THERAPIST]: {
            ids: [],
            labels: [],
            values: []
        },
        [SURE_VIEW_PATIENT]: {
            ids: [],
            labels: [],
            values: []
        }
    };

    const sureTrendCache = {
        [SURE_VIEW_THERAPIST]: createEmptySureTrendCache(),
        [SURE_VIEW_PATIENT]: createEmptySureTrendCache()
    };

    const srsCompositeTrendCache = {
        [SRS_VIEW_THERAPIST]: createEmptySureTrendCache(),
        [SRS_VIEW_PATIENT]: createEmptySureTrendCache()
    };

    function createEmptySureTrendCache() {
        return {
            sessions: [],
            labels: [],
            datasets: []
        };
    }

    function createEmptySrsTrendCache() {
        return {
            sessions: [],
            labels: [],
            datasets: {
                [SRS_METRIC_OVERALL]: [],
                [SRS_METRIC_RELATIONSHIP]: [],
                [SRS_METRIC_GOALS]: [],
                [SRS_METRIC_APPROACH]: []
            }
        };
    }

    const srsTrendCache = {
        [SRS_VIEW_THERAPIST]: createEmptySrsTrendCache(),
        [SRS_VIEW_PATIENT]: createEmptySrsTrendCache()
    };

    const waiCompositeCache = {
        [WAI_ENTITY_THERAPIST]: {
            ids: [],
            labels: [],
            values: []
        },
        [WAI_ENTITY_PATIENT]: {
            labels: [],
            values: []
        }
    };

    function createEmptyTrendCache() {
        return {
            sessions: [],
            labels: [],
            datasets: {
                [WAI_METRIC_TASK]: [],
                [WAI_METRIC_BOND]: [],
                [WAI_METRIC_GOAL]: []
            }
        };
    }

    const waiTrendCache = {
        [WAI_ENTITY_THERAPIST]: createEmptyTrendCache(),
        [WAI_ENTITY_PATIENT]: createEmptyTrendCache()
    };

    const waiCompositeTrendCache = {
        [WAI_ENTITY_THERAPIST]: createEmptySureTrendCache(),
        [WAI_ENTITY_PATIENT]: createEmptySureTrendCache()
    };

    function createEmptyNeqTrendCache() {
        return {
            sessions: [],
            labels: [],
            datasets: {
                [NEQ_METRIC_SEVERITY]: [],
                [NEQ_METRIC_EXPERIENCED]: [],
                [NEQ_METRIC_TREATMENT]: [],
                [NEQ_METRIC_OTHER]: []
            }
        };
    }

    const SURE_DOMAIN_METRIC_DRUG_USE = 'drug_use';
    const SURE_DOMAIN_METRIC_SELF_CARE = 'self_care';
    const SURE_DOMAIN_METRIC_RELATIONSHIPS = 'relationships';
    const SURE_DOMAIN_METRIC_MATERIAL = 'material_resources';
    const SURE_DOMAIN_METRIC_OUTLOOK = 'outlook';

    const SURE_DOMAIN_METRIC_CONFIG = {
        [SURE_DOMAIN_METRIC_DRUG_USE]: {
            column: 'total_sure_drug_use',
            label: 'SURE Drug Use Support',
            range: { min: 6, max: 18 }
        },
        [SURE_DOMAIN_METRIC_SELF_CARE]: {
            column: 'total_sure_self_care',
            label: 'SURE Self Care Support',
            range: { min: 5, max: 15 }
        },
        [SURE_DOMAIN_METRIC_RELATIONSHIPS]: {
            column: 'total_sure_relationships',
            label: 'SURE Relationships Support',
            range: { min: 4, max: 12 }
        },
        [SURE_DOMAIN_METRIC_MATERIAL]: {
            column: 'total_sure_material_resources',
            label: 'SURE Material Resources Support',
            range: { min: 3, max: 9 }
        },
        [SURE_DOMAIN_METRIC_OUTLOOK]: {
            column: 'total_sure_outlook',
            label: 'SURE Outlook Support',
            range: { min: 3, max: 9 }
        }
    };

    const SURE_DOMAIN_METRICS = [
        SURE_DOMAIN_METRIC_DRUG_USE,
        SURE_DOMAIN_METRIC_SELF_CARE,
        SURE_DOMAIN_METRIC_RELATIONSHIPS,
        SURE_DOMAIN_METRIC_MATERIAL,
        SURE_DOMAIN_METRIC_OUTLOOK
    ];

    const sureDomainAggregateCache = {
        [SURE_VIEW_THERAPIST]: {
            ids: [],
            labels: [],
            total_sure_drug_use: [],
            total_sure_self_care: [],
            total_sure_relationships: [],
            total_sure_material_resources: [],
            total_sure_outlook: []
        },
        [SURE_VIEW_PATIENT]: {
            ids: [],
            labels: [],
            total_sure_drug_use: [],
            total_sure_self_care: [],
            total_sure_relationships: [],
            total_sure_material_resources: [],
            total_sure_outlook: []
        }
    };

    function createEmptySureDomainTrendCache() {
        const datasets = {};
        SURE_DOMAIN_METRICS.forEach(metric => {
            datasets[metric] = [];
        });
        return {
            sessions: [],
            labels: [],
            datasets
        };
    }

    const SURE_OUTCOME_METRIC_TOTAL = 'total';
    const SURE_OUTCOME_METRICS = [
        SURE_OUTCOME_METRIC_TOTAL,
        ...SURE_DOMAIN_METRICS
    ];

    const SURE_OUTCOME_METRIC_CONFIG = SURE_DOMAIN_METRICS.reduce((acc, metric) => {
        acc[metric] = SURE_DOMAIN_METRIC_CONFIG[metric];
        return acc;
    }, {
        [SURE_OUTCOME_METRIC_TOTAL]: {
            label: 'SURE Total Score',
            range: { min: 21, max: 63 }
        }
    });

    const SURE_OUTCOME_LEVEL_INTERVENTION = 'intervention';
    const SURE_OUTCOME_LEVEL_SESSION = 'session';

    let currentSureOutcomeView = SURE_VIEW_THERAPIST;
    let currentSureOutcomeLevel = SURE_OUTCOME_LEVEL_INTERVENTION;
    let currentSureOutcomeMetric = SURE_OUTCOME_METRIC_TOTAL;

    function transformSureDomainTrendRecords(records, idKey, labelResolver) {
        const sessionNumbers = Array.from(new Set((records || [])
            .map(row => Number(row.session_id))
            .filter(Number.isFinite))).sort((a, b) => a - b);

        const sessionLabels = sessionNumbers.map(session => `Session ${session}`);
        const sessionIndexMap = new Map();
        sessionNumbers.forEach((session, index) => {
            sessionIndexMap.set(session, index);
        });

        const totalSessions = sessionNumbers.length;
        const dataByIdentifier = new Map();

        (records || []).forEach(row => {
            const rawId = row?.[idKey];
            const identifier = rawId || (idKey === 'therapist_id' ? 'unknown_therapist' : 'Unknown');
            const sessionId = Number(row?.session_id);
            if (!Number.isFinite(sessionId)) {
                return;
            }
            const sessionIndex = sessionIndexMap.get(sessionId);

            if (!dataByIdentifier.has(identifier)) {
                const metricArrays = {};
                SURE_DOMAIN_METRICS.forEach(metric => {
                    metricArrays[metric] = new Array(totalSessions).fill(null);
                });

                dataByIdentifier.set(identifier, {
                    id: identifier,
                    label: labelResolver(identifier),
                    metrics: metricArrays

                });
            }

            const entry = dataByIdentifier.get(identifier);
            SURE_DOMAIN_METRICS.forEach(metric => {
                const columnKey = SURE_DOMAIN_METRIC_CONFIG[metric].column;
                if (Object.prototype.hasOwnProperty.call(row, columnKey)) {
                    entry.metrics[metric][sessionIndex] = toNumericOrNull(row[columnKey]);
                }
            });
        });

        const datasetsByMetric = {};
        SURE_DOMAIN_METRICS.forEach(metric => {
            datasetsByMetric[metric] = [];
        });

        let indexCounter = 0;
        dataByIdentifier.forEach(entry => {
            SURE_DOMAIN_METRICS.forEach(metric => {
                datasetsByMetric[metric].push({
                    id: entry.id,
                    label: entry.label,
                    data: entry.metrics[metric].slice(),
                    index: indexCounter
                });
            });
            indexCounter += 1;
        });

        return {
            sessions: sessionNumbers,
            labels: sessionLabels,
            datasets: datasetsByMetric
        };
    }

    const sureDomainTrendCache = {
        [SURE_VIEW_THERAPIST]: createEmptySureDomainTrendCache(),
        [SURE_VIEW_PATIENT]: createEmptySureDomainTrendCache()
    };

    const PATIENT_TREND_COLOR_PALETTE = [
        { border: 'rgba(54, 162, 235, 1)', background: 'rgba(54, 162, 235, 0.15)' },
        { border: 'rgba(255, 99, 132, 1)', background: 'rgba(255, 99, 132, 0.15)' },
        { border: 'rgba(255, 205, 86, 1)', background: 'rgba(255, 205, 86, 0.15)' },
        { border: 'rgba(75, 192, 192, 1)', background: 'rgba(75, 192, 192, 0.15)' },
        { border: 'rgba(153, 102, 255, 1)', background: 'rgba(153, 102, 255, 0.15)' },
        { border: 'rgba(255, 159, 64, 1)', background: 'rgba(255, 159, 64, 0.15)' }
    ];

    function getTherapistColors(therapistId) {
        return THERAPIST_COLOR_MAP[therapistId] || { background: 'rgba(201, 203, 207, 0.7)', border: 'rgba(201, 203, 207, 1)' };
    }

    function getTherapistDisplayName(therapistId) {
        if (!therapistId) {
            return 'Unknown Therapist';
        }
        return THERAPIST_DISPLAY_NAMES[therapistId] || therapistId;
    }

    function formatSessionsLabel(baseLabel, sessionCount) {
        const labelText = baseLabel && String(baseLabel).trim().length ? String(baseLabel).trim() : 'Unknown';
        const numericCount = Number(sessionCount);
        if (!Number.isFinite(numericCount) || numericCount < 0) {
            return labelText;
        }
        const plural = numericCount === 1 ? 'session' : 'sessions';
        return `${labelText} (${numericCount} ${plural})`;
    }

    function ensureTranscriptModal() {
        const modalElement = document.getElementById('transcriptModal');
        if (!modalElement) {
            transcriptModal = null;
            transcriptModalElement = null;
            return null;
        }

        if (transcriptModalElement !== modalElement) {
            transcriptModal = null;
            transcriptModalElement = modalElement;
        }

        if (!transcriptModal) {
            transcriptModal = new bootstrap.Modal(modalElement);

            if (!modalElement.dataset.modalListenersAttached) {
                modalElement.addEventListener('hide.bs.modal', () => {
                    const activeElement = document.activeElement;
                    if (activeElement && modalElement.contains(activeElement) && typeof activeElement.blur === 'function') {
                        activeElement.blur();
                    }
                });

                modalElement.addEventListener('hidden.bs.modal', () => {
                    if (transcriptModalLastTrigger && typeof transcriptModalLastTrigger.focus === 'function') {
                        transcriptModalLastTrigger.focus();
                    } else if (document.body && typeof document.body.focus === 'function') {
                        document.body.focus();
                    }
                    transcriptModalLastTrigger = null;
                });

                modalElement.dataset.modalListenersAttached = 'true';
            }
        }

        return { modal: transcriptModal, element: modalElement };
    }

    function buildThresholdLabel(content, backgroundColor, textColor = '#ffffff') {
        return {
            display: true,
            content,
            position: 'end',
            backgroundColor,
            color: textColor,
            padding: { top: 4, bottom: 4, left: 6, right: 6 },
            borderRadius: 4
        };
    }
    let selectedAdverseEvents = [];
    let pendingAdverseEvents = [];
    let adverseEventsSelectAllCheckbox = null;

    // Initialize with all events selected
    selectedAdverseEvents = adverseAttributionOptions.map(option => option.value);
    pendingAdverseEvents = [...selectedAdverseEvents];

    let selectedEquityEvents = [];
    let pendingEquityEvents = [];
    let equityEventsSelectAllCheckbox = null;

    selectedEquityEvents = adverseAttributionOptions.map(option => option.value);
    pendingEquityEvents = [...selectedEquityEvents];

    function getAdverseEventLabel(value) {
        const match = adverseAttributionOptions.find(option => option.value === value);
        return match ? match.label : value;
    }

    function normalizePairingValue(value) {
        return (value || '').trim();
    }

    function syncPendingWithSelected() {
        filterKeys.forEach(key => {
            selectedFilters[key] = selectedFilters[key].map(value => String(value));
            pendingFilters[key] = [...selectedFilters[key]];
        });
        pendingPairingId = selectedPairingId;
    }

    function filtersChanged() {
        if (normalizePairingValue(selectedPairingId) !== normalizePairingValue(pendingPairingId)) {
            return true;
        }

        return filterKeys.some(key => {
            if (selectedFilters[key].length !== pendingFilters[key].length) {
                return true;
            }
            return pendingFilters[key].some(value => !selectedFilters[key].includes(value));
        });
    }

    function updateApplyButtonState() {
        if (!applyBtn) return;
        applyBtn.disabled = !filtersChanged();
    }

    function buildFilterParams() {
        const params = new URLSearchParams();

        const pairingValue = normalizePairingValue(selectedPairingId);
        if (pairingValue) {
            pairingValue.split(',').forEach(value => {
                const trimmed = value.trim();
                if (trimmed) {
                    params.append('pairing', trimmed);
                }
            });
            return params;
        }

        if (selectedFilters.therapist.length > 0) {
            selectedFilters.therapist.forEach(value => params.append('therapist', value));
        }
        if (selectedFilters.subtype.length > 0) {
            selectedFilters.subtype.forEach(value => params.append('subtype', value));
        }
        if (selectedFilters.state.length > 0) {
            selectedFilters.state.forEach(value => params.append('state', value));
        }
        if (selectedFilters.session.length > 0) {
            selectedFilters.session.forEach(value => params.append('session', value));
        }

        return params;
    }

    function getWarningConstructLabel(value) {
        const match = warningSignsOptions.find(option => option.value === value);
        return match ? match.label : value;
    }

    function populateWarningSignsDropdown() {
        if (!warningSignsConstructSelect) {
            return;
        }

        warningSignsConstructSelect.innerHTML = '';
        warningSignsOptions.forEach(option => {
            const opt = document.createElement('option');
            opt.value = option.value;
            opt.textContent = option.label;
            warningSignsConstructSelect.appendChild(opt);
        });

        if (selectedWarningConstruct) {
            warningSignsConstructSelect.value = selectedWarningConstruct;
        } else if (warningSignsOptions.length > 0) {
            selectedWarningConstruct = warningSignsOptions[0].value;
            warningSignsConstructSelect.value = selectedWarningConstruct;
        }
    }

    function populateAdverseEventsDropdown() {
        const adverseEventsMenu = document.getElementById('adverseEventsFilterMenu');
        if (!adverseEventsMenu) return;

        adverseEventsMenu.innerHTML = '';

        // Select All checkbox
        const selectAllLi = document.createElement('li');
        selectAllLi.innerHTML = `
            <div class="form-check">
                <input class="form-check-input" type="checkbox" id="adverseEvent_select_all">
                <label class="form-check-label" for="adverseEvent_select_all"><strong>Select All</strong></label>
            </div>
        `;
        adverseEventsMenu.appendChild(selectAllLi);

        adverseEventsSelectAllCheckbox = selectAllLi.querySelector('input');
        adverseEventsSelectAllCheckbox.checked = pendingAdverseEvents.length === adverseAttributionOptions.length;
        adverseEventsSelectAllCheckbox.addEventListener('change', () => {
            handleAdverseSelectAllChange(adverseEventsSelectAllCheckbox.checked);
        });
        selectAllLi.addEventListener('click', e => e.stopPropagation());

        // Individual event checkboxes
        adverseAttributionOptions.forEach(option => {
            const li = document.createElement('li');
            const checkboxId = `adverseEvent_${option.value}`;

            li.innerHTML = `
                <div class="form-check">
                    <input class="form-check-input" type="checkbox" value="${option.value}" id="${checkboxId}" data-event-value="${option.value}">
                    <label class="form-check-label" for="${checkboxId}">${option.label}</label>
                </div>
            `;

            const checkbox = li.querySelector('input');
            checkbox.checked = pendingAdverseEvents.includes(option.value);
            checkbox.addEventListener('change', () => {
                handleAdverseEventPendingChange(option.value, checkbox.checked);
            });

            li.addEventListener('click', e => e.stopPropagation());
            adverseEventsMenu.appendChild(li);
        });

        // Divider
        const divider = document.createElement('li');
        divider.innerHTML = '<hr class="dropdown-divider">';
        adverseEventsMenu.appendChild(divider);

        updateAdverseEventsButtonLabel();
        updateAdverseEventsApplyButtonState();
    }

    function handleAdverseEventPendingChange(value, isChecked) {
        if (isChecked) {
            if (!pendingAdverseEvents.includes(value)) {
                pendingAdverseEvents.push(value);
            }
        } else {
            pendingAdverseEvents = pendingAdverseEvents.filter(v => v !== value);
        }

        if (adverseEventsSelectAllCheckbox) {
            adverseEventsSelectAllCheckbox.checked = pendingAdverseEvents.length === adverseAttributionOptions.length;
        }

        updateAdverseEventsApplyButtonState();
    }

    function handleAdverseSelectAllChange(isChecked) {
        pendingAdverseEvents = isChecked ? adverseAttributionOptions.map(option => option.value) : [];

        const adverseEventsMenu = document.getElementById('adverseEventsFilterMenu');
        const checkboxes = adverseEventsMenu ? Array.from(adverseEventsMenu.querySelectorAll('input[data-event-value]')) : [];
        checkboxes.forEach(box => {
            box.checked = isChecked;
        });

        updateAdverseEventsApplyButtonState();
    }

    function updateAdverseEventsButtonLabel() {
        if (!adverseEventsFilterBtn) {
            return;
        }
        const labelSpan = adverseEventsFilterBtn.querySelector('.filter-label');
        if (!labelSpan) {
            return;
        }

        if (selectedAdverseEvents.length === 0) {
            labelSpan.textContent = 'None Selected';
            return;
        }

        if (selectedAdverseEvents.length === adverseAttributionOptions.length) {
            labelSpan.textContent = 'All Adverse Events';
            return;
        }

        if (selectedAdverseEvents.length === 1) {
            labelSpan.textContent = getAdverseEventLabel(selectedAdverseEvents[0]);
            return;
        }

        labelSpan.textContent = `${selectedAdverseEvents.length} selected`;
    }

    function adverseEventsChanged() {
        if (selectedAdverseEvents.length !== pendingAdverseEvents.length) {
            return true;
        }
        return pendingAdverseEvents.some(value => !selectedAdverseEvents.includes(value));
    }

    function updateAdverseEventsApplyButtonState() {
        if (applyAdverseEventsBtn) {
            applyAdverseEventsBtn.disabled = !adverseEventsChanged();
        }
    }

    function populateEquityEventsDropdown() {
        if (!equityEventsMenu) return;

        equityEventsMenu.innerHTML = '';

        const selectAllLi = document.createElement('li');
        selectAllLi.innerHTML = `
            <div class="form-check">
                <input class="form-check-input" type="checkbox" id="equityEvent_select_all">
                <label class="form-check-label" for="equityEvent_select_all"><strong>Select All</strong></label>
            </div>
        `;
        equityEventsMenu.appendChild(selectAllLi);

        equityEventsSelectAllCheckbox = selectAllLi.querySelector('input');
        equityEventsSelectAllCheckbox.checked = pendingEquityEvents.length === adverseAttributionOptions.length;
        equityEventsSelectAllCheckbox.addEventListener('change', () => {
            handleEquitySelectAllChange(equityEventsSelectAllCheckbox.checked);
        });
        selectAllLi.addEventListener('click', e => e.stopPropagation());

        adverseAttributionOptions.forEach(option => {
            const li = document.createElement('li');
            const checkboxId = `equityEvent_${option.value}`;

            li.innerHTML = `
                <div class="form-check">
                    <input class="form-check-input" type="checkbox" value="${option.value}" id="${checkboxId}" data-equity-event-value="${option.value}">
                    <label class="form-check-label" for="${checkboxId}">${option.label}</label>
                </div>
            `;

            const checkbox = li.querySelector('input');
            checkbox.checked = pendingEquityEvents.includes(option.value);
            checkbox.addEventListener('change', () => {
                handleEquityEventPendingChange(option.value, checkbox.checked);
            });

            li.addEventListener('click', e => e.stopPropagation());
            equityEventsMenu.appendChild(li);
        });

        const divider = document.createElement('li');
        divider.innerHTML = '<hr class="dropdown-divider">';
        equityEventsMenu.appendChild(divider);

        updateEquityEventsButtonLabel();
        updateEquityEventsApplyButtonState();
    }

    function handleEquityEventPendingChange(value, isChecked) {
        if (isChecked) {
            if (!pendingEquityEvents.includes(value)) {
                pendingEquityEvents.push(value);
            }
        } else {
            pendingEquityEvents = pendingEquityEvents.filter(v => v !== value);
        }

        if (equityEventsSelectAllCheckbox) {
            equityEventsSelectAllCheckbox.checked = pendingEquityEvents.length === adverseAttributionOptions.length;
        }

        updateEquityEventsApplyButtonState();
    }

    function handleEquitySelectAllChange(isChecked) {
        pendingEquityEvents = isChecked ? adverseAttributionOptions.map(option => option.value) : [];

        if (equityEventsMenu) {
            equityEventsMenu.querySelectorAll('input[data-equity-event-value]').forEach(box => {
                box.checked = isChecked;
            });
        }

        updateEquityEventsApplyButtonState();
    }

    function updateEquityEventsButtonLabel() {
        if (!equityEventsFilterBtn) {
            return;
        }
        const labelSpan = equityEventsFilterBtn.querySelector('.filter-label');
        if (!labelSpan) {
            return;
        }

        if (selectedEquityEvents.length === 0) {
            labelSpan.textContent = 'None Selected';
            return;
        }

        if (selectedEquityEvents.length === adverseAttributionOptions.length) {
            labelSpan.textContent = 'All Adverse Events';
            return;
        }

        if (selectedEquityEvents.length === 1) {
            labelSpan.textContent = getAdverseEventLabel(selectedEquityEvents[0]);
            return;
        }

        labelSpan.textContent = `${selectedEquityEvents.length} selected`;
    }

    function equityEventsChanged() {
        if (selectedEquityEvents.length !== pendingEquityEvents.length) {
            return true;
        }
        return pendingEquityEvents.some(value => !selectedEquityEvents.includes(value));
    }

    function updateEquityEventsApplyButtonState() {
        if (applyEquityEventsBtn) {
            applyEquityEventsBtn.disabled = !equityEventsChanged();
        }
    }

    let warningSignsClickHandlerBound = false;
    let warningSignsAggregatedState = null;
    let warningSignsActiveSession = null;
    let warningSignsActiveSessionDetails = [];
    let warningSignsSessionDetails = {};
    let warningSignsSessionOrder = [];

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
        const safe = escapeHtml(value);
        return safe.replace(/\n/g, '<br>');
    }

    function formatPercentage(value, digits = 1) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return '—';
        }
        return `${numeric.toFixed(digits)}%`;
    }

    function formatSeverity(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return '—';
        }
        return numeric.toFixed(2);
    }

    function formatEquitySelectionSummary(selectedValues) {
        if (!Array.isArray(selectedValues) || selectedValues.length === 0) {
            return 'None Selected';
        }

        if (selectedValues.length === adverseAttributionOptions.length) {
            return 'All Adverse Events';
        }

        const labels = selectedValues
            .map(getAdverseEventLabel)
            .filter(label => typeof label === 'string' && label.trim().length > 0);

        if (labels.length === 0) {
            return 'Custom Selection';
        }

        if (labels.length === 1) {
            return labels[0];
        }

        if (labels.length === 2) {
            return `${labels[0]} and ${labels[1]}`;
        }

        if (labels.length === 3) {
            return `${labels[0]}, ${labels[1]}, and ${labels[2]}`;
        }

        const remainingCount = labels.length - 2;
        return `${labels[0]}, ${labels[1]}, and ${remainingCount} others`;
    }

    if (applyBtn) {
        applyBtn.disabled = true;
    }

    // --- FILTER POPULATION ---
    function populateFilters() {
        fetch('/api/filters')
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load filter options');
                }
                return res.json();
            })
            .then(data => {
                const normalizedData = {
                    therapist: Array.isArray(data.therapists) ? data.therapists.map(String) : [],
                    subtype: Array.isArray(data.subtypes) ? data.subtypes.map(String) : [],
                    state: Array.isArray(data.states) ? data.states.map(String) : [],
                    session: Array.isArray(data.sessions) ? data.sessions.map(String) : []
                };

                filterKeys.forEach(filterName => {
                    availableFilterOptions[filterName] = Array.from(new Set(normalizedData[filterName]));
                    const normalizedSelected = selectedFilters[filterName]
                        .map(String)
                        .filter(value => availableFilterOptions[filterName].includes(value));

                    if (normalizedSelected.length === 0 && availableFilterOptions[filterName].length > 0) {
                        selectedFilters[filterName] = [...availableFilterOptions[filterName]];
                    } else {
                        selectedFilters[filterName] = normalizedSelected;
                    }
                });

                syncPendingWithSelected();

                populateCheckboxFilter('therapist', availableFilterOptions.therapist, 'All Therapists');
                populateCheckboxFilter('subtype', availableFilterOptions.subtype, 'All Subtypes');
                populateCheckboxFilter('state', availableFilterOptions.state, 'All Stages');
                populateCheckboxFilter('session', availableFilterOptions.session, 'All Sessions');
                filterKeys.forEach(updateFilterButtonLabel);
                if (pairingFilterInput) {
                    pairingFilterInput.value = selectedPairingId;
                }
                updateApplyButtonState();
            })
            .catch(error => console.error('Error loading filter options:', error))
            .finally(() => {
                if (filtersReadyResolve) {
                    filtersReadyResolve();
                    filtersReadyResolve = null;
                }
            });

    }

    function populateCheckboxFilter(filterName, items, label) {
        const container = filters[filterName];
        if (!container) return;
        container.innerHTML = '';

        const normalizedItems = Array.isArray(items) ? items.map(String) : [];

        // Add Select All checkbox
        const selectAllLi = document.createElement('li');
        const selectAllId = `${filterName}_select_all`;
        selectAllLi.innerHTML = `
            <div class="form-check">
                <input class="form-check-input" type="checkbox" id="${selectAllId}">
                <label class="form-check-label" for="${selectAllId}"><strong>Select All</strong></label>
            </div>
        `;
        container.appendChild(selectAllLi);

        const selectAllCheckbox = selectAllLi.querySelector('input');
        const totalOptions = normalizedItems.length;
        const selectedCount = pendingFilters[filterName].length;
        const allSelected = totalOptions > 0 && selectedCount === totalOptions;
        const hasSelection = selectedCount > 0;
        selectAllCheckbox.checked = allSelected;
        selectAllCheckbox.indeterminate = hasSelection && !allSelected;
        selectAllCheckbox.addEventListener('change', () => {
            handleGlobalSelectAllChange(filterName, selectAllCheckbox.checked);
        });
        selectAllLi.addEventListener('click', e => e.stopPropagation());

        // Add individual checkboxes
        normalizedItems.forEach(item => {
            const li = document.createElement('li');
            const checkboxId = `${filterName}_${item}`;
            const displayLabel = filterName === 'therapist' ? getTherapistDisplayName(item) : item;

            li.innerHTML = `
                <div class="form-check">
                    <input class="form-check-input" type="checkbox" value="${item}" id="${checkboxId}" data-filter="${filterName}">
                    <label class="form-check-label" for="${checkboxId}">${displayLabel}</label>
                </div>
            `;

            container.appendChild(li);

            const checkbox = li.querySelector('input');
            checkbox.checked = pendingFilters[filterName].includes(item);
            checkbox.addEventListener('change', function() {
                handleFilterChange(filterName, item, this.checked);
                // Update select all checkbox state
                const currentCount = pendingFilters[filterName].length;
                selectAllCheckbox.checked = totalOptions > 0 && currentCount === totalOptions;
                selectAllCheckbox.indeterminate = currentCount > 0 && currentCount < totalOptions;
            });

            li.addEventListener('click', e => e.stopPropagation());
        });
    }

    function applySelectionsToFilter(filterName, values) {
        const container = filters[filterName];
        if (!container) {
            return;
        }

        const stringValues = values.map(value => String(value));
        const valueSet = new Set(stringValues);
        const checkboxes = Array.from(container.querySelectorAll('input[data-filter]'));
        checkboxes.forEach(box => {
            box.checked = valueSet.has(box.value);
        });

        const selectAllCheckbox = document.getElementById(`${filterName}_select_all`);
        if (selectAllCheckbox) {
            const selectableCount = checkboxes.length;
            const selectedCount = stringValues.length;
            const allSelected = selectableCount > 0 && checkboxes.every(cb => valueSet.has(cb.value));
            selectAllCheckbox.checked = allSelected;
            selectAllCheckbox.indeterminate = !allSelected && selectedCount > 0;
        }
    }

    async function fetchPairingContext(pairingValue) {
        const params = new URLSearchParams();
        pairingValue.split(',').forEach(token => {
            const trimmed = token.trim();
            if (trimmed) {
                params.append('pairing', trimmed);
            }
        });

        if ([...params.keys()].length === 0) {
            return null;
        }

        const response = await fetch(`/api/pairings/context?${params.toString()}`, { credentials: 'same-origin' });
        if (!response.ok) {
            throw new Error(`Failed to load pairing context (status ${response.status})`);
        }
        return response.json();
    }

    async function applyPairingContext(pairingValue) {
        await filtersReadyPromise;
        const context = await fetchPairingContext(pairingValue);

        selectedPairingId = pairingValue;

        if (!context || !Array.isArray(context.pairings) || context.pairings.length === 0) {
            console.warn('No pairing context found for value:', pairingValue);
            selectedFilters.therapist = [];
            selectedFilters.subtype = [];
            selectedFilters.state = [];
            selectedFilters.session = [];
        } else {
            selectedFilters.therapist = Array.isArray(context.therapists)
                ? context.therapists.map(String).filter(value => availableFilterOptions.therapist.includes(value))
                : [];
            selectedFilters.subtype = Array.isArray(context.subtypes)
                ? context.subtypes.map(String).filter(value => availableFilterOptions.subtype.includes(value))
                : [];
            selectedFilters.state = Array.isArray(context.states)
                ? context.states.map(String).filter(value => availableFilterOptions.state.includes(value))
                : [];
            selectedFilters.session = [];
        }

        syncPendingWithSelected();

        filterKeys.forEach(filterName => {
            applySelectionsToFilter(filterName, selectedFilters[filterName]);
            updateFilterButtonLabel(filterName);
        });

        if (pairingFilterInput) {
            pairingFilterInput.value = pairingValue;
        }
    }

    function handleFilterChange(filterName, value, isChecked) {
        if (isChecked) {
            if (!pendingFilters[filterName].includes(value)) {
                pendingFilters[filterName].push(value);
            }
        } else {
            pendingFilters[filterName] = pendingFilters[filterName].filter(v => v !== value);
        }
        updateApplyButtonState();
    }

    function handleGlobalSelectAllChange(filterName, isChecked) {
        const container = filters[filterName];
        if (!container) {
            return;
        }

        const options = availableFilterOptions[filterName] || [];
        pendingFilters[filterName] = isChecked ? [...options] : [];

        const checkboxes = container.querySelectorAll(`input[data-filter="${filterName}"]`);
        checkboxes.forEach(box => {
            box.checked = isChecked;
        });

        const selectAllCheckbox = container.querySelector(`#${filterName}_select_all`);
        if (selectAllCheckbox) {
            selectAllCheckbox.checked = isChecked && options.length > 0;
            selectAllCheckbox.indeterminate = false;
        }

        updateApplyButtonState();
    }

    function updateFilterButtonLabel(filterName) {
        const button = filterButtons[filterName];
        if (!button) return;
        const labelSpan = button.querySelector('.filter-label');
        const count = selectedFilters[filterName].length;
        const totalAvailable = availableFilterOptions[filterName]?.length || 0;

        const labels = {
            therapist: 'All Therapists',
            subtype: 'All Subtypes',
            state: 'All Stages',
            session: 'All Sessions'
        };

        if (count === 0 || (totalAvailable > 0 && count === totalAvailable)) {
            labelSpan.textContent = labels[filterName];
        } else if (count === 1) {
            const selectedValue = selectedFilters[filterName][0];
            labelSpan.textContent = filterName === 'therapist'
                ? getTherapistDisplayName(selectedValue)
                : selectedValue;
        } else {
            labelSpan.textContent = `${count} selected`;
        }
    }

    function startLoadingCycle() {
        pendingRequests = 0;
        if (loadingOverlay) {
            loadingOverlay.style.display = 'flex';
        }
    }

    function beginLoading() {
        pendingRequests += 1;
        if (loadingOverlay) {
            loadingOverlay.style.display = 'flex';
        }
    }

    function endLoading() {
        pendingRequests = Math.max(0, pendingRequests - 1);
        if (pendingRequests === 0 && loadingOverlay) {
            loadingOverlay.style.display = 'none';
        }
    }

    // --- INITIALIZATION ---
    function initialize() {
        ensureTranscriptModal();

        loadingOverlay = document.getElementById('loadingOverlay');
        if (loadingOverlay) {
            loadingOverlay.style.display = 'none';
        }

        populateWarningSignsDropdown();
        populateAdverseEventsDropdown();
        populateEquityEventsDropdown();
        populateFilters();
        setupEventListeners();
        setupEquityViewToggle();
        setupNeqToggles();
        renderSureOutcomeChart();
        renderSrsOutcomeChart();
        renderWaiUnifiedChart();

        // Delay initial chart load slightly to allow filters to populate
        setTimeout(updateDashboard, 300);
    }

    // --- DASHBOARD UPDATE LOGIC ---
    function updateDashboard() {
        startLoadingCycle();

        const params = buildFilterParams();
        const query = params.toString();

        resetNeqCaches();
        renderNeqUnifiedChart();

        updateDashboardSummary(query);
        updateCrisisData(query);
        updateActionPlanChart(query);
        updateOverallAdherenceChart(query);
        updateTherapistComparisonChart(query);
        updateTherapistNeqChart(query);
        updateNeqAggregateChartData(query);
        updateNeqSessionTrends(query);
        updateNeqQuestionTable(query);
        updateTherapistSureChart(query);
        updateTherapistWaiChart(query);
        updateMiGlobalProfileChart(query);
        updateMiGlobalMetrics(query);
        updateMiBehaviorMetrics(query);
        updateSrsComponentTrends(query);
        updateWaiComponentTrends(query);
        updateAdverseOutcomesChart(query);
        updateAdverseAttributionsChart(params);
        updateInSessionWarningSignsChart(params);
        updatePatientRetentionChart(query);
        updateScoreTrends(query);
        updateSureSessionTrends(query);
        updateSureDomainTrendData(query);
        updateSureDomainAggregateChartData(query);
        updatePatientTypeComparisons(query);
        updateEquityAudit(params);
    }

    // --- INDIVIDUAL CHART UPDATE FUNCTIONS ---
    function updateCrisisData(query) {
        beginLoading();
        fetch(`/api/crisis-events?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load crisis events');
                }
                return res.json();
            })
            .then(data => {
                const filteredData = data;
                
                const counts = filteredData.reduce((acc, item) => {
                    acc[item.classification] = (acc[item.classification] || 0) + 1;
                    return acc;
                }, {});

                createOrUpdateChart('crisisClassificationChart', 'pie', {
                    labels: Object.keys(counts),
                    datasets: [{ data: Object.values(counts) }]
                }, { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } } });

                crisisList.innerHTML = '';
                const topCrises = data.filter(e => e.classification !== 'No Crisis').slice(0, 10);
                if (topCrises.length === 0) {
                    crisisList.innerHTML = '<li class="list-group-item text-muted">No crisis events found for this filter.</li>';
                } else {
                    topCrises.forEach(event => {
                        const li = document.createElement('li');
                        li.className = 'list-group-item list-group-item-action';
                        li.style.cursor = 'pointer';
                        const pairingIdRaw = event.pairing_id;
                        const sessionIdRaw = event.session_id;
                        const turnRaw = event.turn;

                        const pairingText = pairingIdRaw !== undefined && pairingIdRaw !== null ? String(pairingIdRaw) : 'Unknown';
                        const sessionText = sessionIdRaw !== undefined && sessionIdRaw !== null ? String(sessionIdRaw) : 'Unknown';
                        const turnText = turnRaw !== undefined && turnRaw !== null ? String(turnRaw) : 'Unknown';
                        const subtypeRaw = event.patient_subtype || event.patient_name || 'Unknown';

                        const pairingLabel = escapeHtml(pairingText);
                        const sessionLabel = escapeHtml(sessionText);
                        const turnLabel = escapeHtml(turnText);
                        const subtypeLabel = escapeHtml(subtypeRaw);

                        const therapistId = event.therapist_id;
                        let therapistDisplay = '';
                        if (therapistId) {
                            therapistDisplay = getTherapistDisplayName(therapistId);
                        }
                        if (!therapistDisplay || therapistDisplay === therapistId) {
                            therapistDisplay = event.therapist_label || event.therapist || therapistDisplay;
                        }
                        if (!therapistDisplay || !String(therapistDisplay).trim()) {
                            therapistDisplay = 'Unknown Therapist';
                        }
                        const therapistLabel = escapeHtml(therapistDisplay);

                        const classificationLabel = escapeHtml(event.classification || 'Crisis Event');
                        li.innerHTML = `
                            <div class="fw-semibold">${classificationLabel}</div>
                            <small class="text-muted d-block">Pairing ID: ${pairingLabel} · Patient Type: ${subtypeLabel} · Therapist: ${therapistLabel} · Session: ${sessionLabel} · Dialogue Turn: ${turnLabel}</small>
                        `;
                        li.onclick = () => showTranscript(pairingIdRaw, subtypeRaw, therapistDisplay, sessionIdRaw, turnRaw);
                        crisisList.appendChild(li);
                    });
                }
            })
            .catch(error => console.error('Error fetching crisis events:', error))
            .finally(() => endLoading());
    }

    function updateDashboardSummary(query) {
        beginLoading();
        fetch(`/api/dashboard-summary?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load dashboard summary');
                }
                return res.json();
            })
            .then(data => {
                if (!data) return;
                const { therapists, personas, sessions, patient_turns, therapist_turns, dialogue_turns } = data;
                const therapistTotal = Number(therapists || 0);
                const personaTotal = Number(personas || 0);
                const sessionTotal = Number(sessions || 0);
                const patientTurns = Number(patient_turns || 0);
                const therapistTurns = Number(therapist_turns || 0);
                const totalTurns = dialogue_turns != null ? Number(dialogue_turns) : patientTurns + therapistTurns;

                if (summaryDisplay.therapists) {
                    summaryDisplay.therapists.textContent = therapistTotal.toLocaleString();
                }
                if (summaryDisplay.personas) {
                    summaryDisplay.personas.textContent = personaTotal.toLocaleString();
                }
                if (summaryDisplay.sessions) {
                    summaryDisplay.sessions.textContent = sessionTotal.toLocaleString();
                }
                if (summaryDisplay.turns) {
                    summaryDisplay.turns.textContent = (Number(totalTurns || 0)).toLocaleString();
                }
            })
            .catch(error => console.error('Error fetching dashboard summary:', error))
            .finally(() => endLoading());
    }
    
    function updateActionPlanChart(query) {
        beginLoading();
        fetch(`/api/action-plan-adherence?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load action plan adherence data');
                }
                return res.json();
            }).then(apiData => {
                createOrUpdateChart('actionPlanChart', 'bar', {
                    labels: apiData.map(d => d.step),
                    datasets: [{ label: '% Adherence', data: apiData.map(d => d.percentage) }]
                }, {
                    responsive: true, maintainAspectRatio: false,
                    scales: { y: { beginAtZero: true, max: 100, title: { display: true, text: 'Adherence %' } } },
                    plugins: { legend: { display: false } }
                });
            })
            .catch(error => console.error('Error fetching action plan data:', error))
            .finally(() => endLoading());
    }

    function updateOverallAdherenceChart(query) {
        beginLoading();
        fetch(`/api/overall-adherence?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load overall adherence');
                }
                return res.json();
            }).then(data => {
                const percentage = data.percentage;
                document.getElementById('overallAdherenceText').textContent = `${percentage.toFixed(1)}%`;

                createOrUpdateChart('overallAdherenceChart', 'doughnut', {
                    labels: ['Fully Adherent', 'Not Fully Adherent'],
                    datasets: [{
                        data: [percentage, 100 - percentage],
                        backgroundColor: ['rgba(75, 192, 192, 0.7)', 'rgba(230, 230, 230, 0.7)'],
                        borderColor: ['#fff'], borderWidth: 2
                    }]
                }, {
                    responsive: true, maintainAspectRatio: false, cutout: '70%',
                    plugins: { legend: { display: false }, tooltip: { enabled: false } }
                });
            })
            .catch(error => console.error('Error fetching overall adherence:', error))
            .finally(() => endLoading());
    }

    function updateTherapistComparisonChart(query) {
        beginLoading();
        fetch(`/api/therapist-comparison?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load therapist comparison data');
                }
                return res.json();
            }).then(apiData => {
                const filteredData = (apiData || []).filter(d => d.therapist_id !== 'therapist_psych_material');
                const rawTherapistLabels = filteredData.map(d => getTherapistDisplayName(d.therapist_id));
                const sessionCounts = filteredData.map(d => Number(d.session_count) || 0);
                const therapistLabels = rawTherapistLabels.map((label, index) => formatSessionsLabel(label, sessionCounts[index]));
                const toNumberOrNull = value => (value === null || value === undefined ? null : Number(value));
                const compositeValues = filteredData.map(d => toNumberOrNull(d.average_srs_score));
                const overallValues = filteredData.map(d => toNumberOrNull(d.avg_srs_overall));
                const relationshipValues = filteredData.map(d => toNumberOrNull(d.avg_srs_relationship));
                const goalsValues = filteredData.map(d => toNumberOrNull(d.avg_srs_goals));
                const approachValues = filteredData.map(d => toNumberOrNull(d.avg_srs_approach));

                srsCompositeCache[SRS_VIEW_THERAPIST] = {
                    ids: filteredData.map(d => d.therapist_id),
                    labels: therapistLabels.slice(),
                    rawLabels: rawTherapistLabels.slice(),
                    sessionCounts: sessionCounts.slice(),
                    values: compositeValues.slice()
                };

                srsComparisonCache[SRS_VIEW_THERAPIST] = {
                    ids: filteredData.map(d => d.therapist_id),
                    labels: therapistLabels.slice(),
                    rawLabels: rawTherapistLabels.slice(),
                    sessionCounts: sessionCounts.slice(),
                    [SRS_METRIC_OVERALL]: overallValues.slice(),
                    [SRS_METRIC_RELATIONSHIP]: relationshipValues.slice(),
                    [SRS_METRIC_GOALS]: goalsValues.slice(),
                    [SRS_METRIC_APPROACH]: approachValues.slice()
                };

                if (currentSrsOutcomeView === SRS_VIEW_THERAPIST) {
                    renderSrsOutcomeChart();
                }
            })
            .catch(error => console.error('Error fetching therapist comparison:', error))
            .finally(() => endLoading());
    }

    function updateTherapistNeqChart(query) {
        beginLoading();
        fetch(`/api/therapist-comparison-neq?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load NEQ comparison data');
                }
                return res.json();
            })
            .then(apiData => {
                const records = Array.isArray(apiData) ? apiData : [];
                const severityOrder = records.map(item => item.therapist_id);
                const existingOrder = neqInterventionOrder[NEQ_VIEW_THERAPIST].filter(key => !severityOrder.includes(key));
                neqInterventionOrder[NEQ_VIEW_THERAPIST] = [...severityOrder, ...existingOrder];

                records.forEach(item => {
                    const therapistId = item.therapist_id;
                    const rawLabel = getTherapistDisplayName(therapistId);
                    const sessionCount = Number(item.session_count) || 0;
                    const label = formatSessionsLabel(rawLabel, sessionCount);
                    setNeqMetric(NEQ_VIEW_THERAPIST, therapistId, label, NEQ_METRIC_SEVERITY, item.avg_neq_score, therapistId);
                });

                if (currentNeqMode === NEQ_MODE_INTERVENTION && currentNeqView === NEQ_VIEW_THERAPIST) {
                    renderNeqUnifiedChart();
                }
            })
            .catch(error => console.error('Error fetching NEQ comparison:', error))
            .finally(() => endLoading());
    }

    function updateNeqToggleStyles() {
        if (neqModeToggleButtons.length) {
            neqModeToggleButtons.forEach(button => {
                const modeValue = button.dataset.neqMode;
                const isActive = modeValue === currentNeqMode;
                button.classList.toggle('active', isActive);
                button.classList.toggle('btn-primary', isActive);
                button.classList.toggle('btn-outline-primary', !isActive);
            });
        }

        if (neqViewToggleButtons.length) {
            neqViewToggleButtons.forEach(button => {
                const viewValue = button.dataset.neqView;
                const isActive = viewValue === currentNeqView;
                button.classList.toggle('active', isActive);
                button.classList.toggle('btn-primary', isActive);
                button.classList.toggle('btn-outline-primary', !isActive);
            });
        }

        if (neqMetricToggleButtons.length) {
            neqMetricToggleButtons.forEach(button => {
                const metricValue = button.dataset.neqMetric;
                const isActive = metricValue === currentNeqMetric;
                button.classList.toggle('active', isActive);
                button.classList.toggle('btn-secondary', isActive);
                button.classList.toggle('btn-outline-secondary', !isActive);
            });
        }
    }

    function setupNeqToggles() {
        if (neqModeToggleButtons.length) {
            neqModeToggleButtons.forEach(button => {
                button.addEventListener('click', () => {
                    const modeValue = button.dataset.neqMode;
                    if (!modeValue || (modeValue !== NEQ_MODE_INTERVENTION && modeValue !== NEQ_MODE_SESSION)) {
                        return;
                    }
                    if (modeValue === currentNeqMode) {
                        return;
                    }
                    currentNeqMode = modeValue;
                    renderNeqUnifiedChart();
                });
            });
        }

        if (neqViewToggleButtons.length) {
            neqViewToggleButtons.forEach(button => {
                button.addEventListener('click', () => {
                    const viewValue = button.dataset.neqView;
                    if (!viewValue || (viewValue !== NEQ_VIEW_THERAPIST && viewValue !== NEQ_VIEW_PATIENT)) {
                        return;
                    }
                    if (viewValue === currentNeqView) {
                        return;
                    }
                    currentNeqView = viewValue;
                    renderNeqUnifiedChart();
                });
            });
        }

        if (neqMetricToggleButtons.length) {
            neqMetricToggleButtons.forEach(button => {
                button.addEventListener('click', () => {
                    const metricValue = button.dataset.neqMetric;
                    if (!metricValue || !NEQ_METRICS.includes(metricValue)) {
                        return;
                    }
                    if (metricValue === currentNeqMetric) {
                        return;
                    }
                    currentNeqMetric = metricValue;
                    renderNeqUnifiedChart();
                });
            });
        }

        updateNeqToggleStyles();
        renderNeqUnifiedChart();
    }

    function renderNeqUnifiedChart() {
        updateNeqToggleStyles();

        const metricLabel = NEQ_METRIC_LABELS[currentNeqMetric] || 'NEQ Metric';
        const axisConfig = NEQ_METRIC_AXIS[currentNeqMetric] || { min: 0, max: 32, step: 4 };

        if (currentNeqMode === NEQ_MODE_INTERVENTION) {
            const cache = neqInterventionCache[currentNeqView];
            const order = neqInterventionOrder[currentNeqView] || [];
            const records = order.map(key => cache.get(key)).filter(Boolean);

            const labels = records.map(record => record.label);
            const values = records.map(record => toNumericOrNull(record[currentNeqMetric]));

            if (!labels.length) {
                createOrUpdateChart('neqOutcomesChart', 'bar', {
                    labels: [],
                    datasets: []
                }, {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: {
                            min: axisConfig.min,
                            max: axisConfig.max,
                            title: { display: true, text: metricLabel },
                            ticks: {
                                stepSize: axisConfig.step,
                                callback: value => `${value}`
                            }
                        }
                    },
                    plugins: { legend: { display: false } }
                });
                return;
            }

            const colors = records.map((record, index) => {
                if (currentNeqView === NEQ_VIEW_THERAPIST) {
                    return getTherapistColors(record.colorId || record.key);
                }
                return getPatientBarColors(index);
            });

            createOrUpdateChart('neqOutcomesChart', 'bar', {
                labels,
                datasets: [{
                    label: `${metricLabel} (${currentNeqView === NEQ_VIEW_THERAPIST ? 'Therapist Average' : 'Patient Subtype Average'})`,
                    data: values,
                    backgroundColor: colors.map(color => color.background),
                    borderColor: colors.map(color => color.border),
                    borderWidth: 1
                }]
            }, {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        min: axisConfig.min,
                        max: axisConfig.max,
                        title: { display: true, text: metricLabel },
                        ticks: {
                            stepSize: axisConfig.step,
                            callback: value => `${value}`
                        }
                    }
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: context => {
                                const value = context.parsed.x;
                                if (value === null || value === undefined || Number.isNaN(value)) {
                                    return 'No data';
                                }
                                return `${metricLabel}: ${Number(value).toFixed(2)}`;
                            }
                        }
                    }
                }
            });
            return;
        }

        const cache = neqTrendCache[currentNeqView] || createEmptyNeqTrendCache();
        const labels = Array.isArray(cache.labels) ? cache.labels.slice() : [];
        const datasetDefs = cache.datasets?.[currentNeqMetric] || [];
        const datasets = datasetDefs.map(series => {
            const palette = currentNeqView === NEQ_VIEW_THERAPIST
                ? getTherapistColors(series.colorId || series.id)
                : getPatientTrendColor(series.index);
            return {
                label: series.label,
                data: Array.isArray(series.data) ? series.data.slice() : [],
                borderColor: palette.border,
                backgroundColor: palette.background,
                tension: 0.3,
                fill: false,
                spanGaps: true,
                borderWidth: 2,
                pointRadius: 3,
                pointHoverRadius: 4
            };
        });

        createOrUpdateChart('neqOutcomesChart', 'line', {
            labels,
            datasets
        }, {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { title: { display: true, text: 'Session' } },
                y: {
                    min: axisConfig.min,
                    max: axisConfig.max,
                    title: { display: true, text: metricLabel },
                    ticks: {
                        stepSize: axisConfig.step,
                        callback: value => `${value}`
                    }
                }
            },
            plugins: {
                legend: { position: 'bottom', labels: { boxWidth: 12 } },
                tooltip: {
                    callbacks: {
                        label: context => {
                            const value = context.parsed.y;
                            if (value === null || value === undefined || Number.isNaN(value)) {
                                return `${context.dataset.label}: No data`;
                            }
                            return `${context.dataset.label}: ${Number(value).toFixed(2)}`;
                        }
                    }
                }
            }
        });
    }

    function ensureNumericValue(value) {
        const numeric = Number(value);
        return Number.isFinite(numeric) ? numeric : 0;
    }

    function normalizeNeqAggregateData(rawData, view) {
        if (!Array.isArray(rawData)) {
            return [];
        }

        return rawData.map(item => {
            const rawKey = item?.group_key ?? item?.key ?? '';
            const key = (rawKey === null || rawKey === undefined || rawKey === '') ? 'Unknown' : rawKey;
            let label;

            if (view === NEQ_VIEW_THERAPIST) {
                label = item?.label || item?.group_label || getTherapistDisplayName(key) || 'Unknown Therapist';
            } else {
                if (item?.label) {
                    label = item.label;
                } else if (key === 'Unknown') {
                    label = 'Unknown Subtype';
                } else {
                    label = key || 'Unknown Subtype';
                }
            }

            const sessionCount = Number(item?.session_count) || 0;
            const displayLabel = formatSessionsLabel(label, sessionCount);

            return {
                key,
                label: displayLabel,
                rawLabel: label,
                sessionCount,
                experienced: ensureNumericValue(item?.avg_effects_experienced),
                treatment: ensureNumericValue(item?.avg_due_to_treatment),
                other: ensureNumericValue(item?.avg_due_to_other)
            };
        });
    }

    function updateNeqAggregateChartData(query) {
        const buildUrl = view => {
            const viewParam = view === NEQ_VIEW_PATIENT ? 'subtype' : 'therapist';
            const base = `/api/neq-aggregate-breakdown?view=${encodeURIComponent(viewParam)}`;
            return query ? `${base}&${query}` : base;
        };

        beginLoading();
        Promise.all([
            fetch(buildUrl(NEQ_VIEW_THERAPIST)).then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load NEQ aggregate data (therapist)');
                }
                return res.json();
            }),
            fetch(buildUrl(NEQ_VIEW_PATIENT)).then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load NEQ aggregate data (patient subtype)');
                }
                return res.json();
            })
        ])
            .then(([therapistData, patientData]) => {
                const therapistNormalized = normalizeNeqAggregateData(therapistData, NEQ_VIEW_THERAPIST);
                const patientNormalized = normalizeNeqAggregateData(patientData, NEQ_VIEW_PATIENT);

                therapistNormalized.forEach(item => {
                    setNeqMetric(NEQ_VIEW_THERAPIST, item.key, item.label, NEQ_METRIC_EXPERIENCED, item.experienced, item.key);
                    setNeqMetric(NEQ_VIEW_THERAPIST, item.key, item.label, NEQ_METRIC_TREATMENT, item.treatment, item.key);
                    setNeqMetric(NEQ_VIEW_THERAPIST, item.key, item.label, NEQ_METRIC_OTHER, item.other, item.key);
                });

                const therapistKeys = therapistNormalized.map(item => item.key);
                const existingTherapistKeys = neqInterventionOrder[NEQ_VIEW_THERAPIST].filter(key => !therapistKeys.includes(key));
                neqInterventionOrder[NEQ_VIEW_THERAPIST] = [...therapistKeys, ...existingTherapistKeys];

                patientNormalized.forEach(item => {
                    setNeqMetric(NEQ_VIEW_PATIENT, item.key, item.label, NEQ_METRIC_EXPERIENCED, item.experienced);
                    setNeqMetric(NEQ_VIEW_PATIENT, item.key, item.label, NEQ_METRIC_TREATMENT, item.treatment);
                    setNeqMetric(NEQ_VIEW_PATIENT, item.key, item.label, NEQ_METRIC_OTHER, item.other);
                });

                const patientKeys = patientNormalized.map(item => item.key);
                const existingPatientKeys = neqInterventionOrder[NEQ_VIEW_PATIENT].filter(key => !patientKeys.includes(key));
                neqInterventionOrder[NEQ_VIEW_PATIENT] = [...patientKeys, ...existingPatientKeys];

                if (currentNeqMode === NEQ_MODE_INTERVENTION) {
                    const waitingOnPatientSeverity = currentNeqView === NEQ_VIEW_PATIENT && currentNeqMetric === NEQ_METRIC_SEVERITY;
                    if (!waitingOnPatientSeverity) {
                        renderNeqUnifiedChart();
                    }
                }
            })
            .catch(error => console.error('Error fetching NEQ aggregate breakdown:', error))
            .finally(() => endLoading());
    }

    function transformNeqTrendRecords(records, idKey, labelResolver) {
        const sessions = Array.from(new Set((records || [])
            .map(row => Number(row?.session_id))
            .filter(Number.isFinite))).sort((a, b) => a - b);

        const sessionLabels = sessions.map(session => `Session ${session}`);
        const sessionIndexMap = new Map();
        sessions.forEach((session, index) => {
            sessionIndexMap.set(session, index);
        });

        const totalSessions = sessions.length;
        const dataByIdentifier = new Map();

        (records || []).forEach(row => {
            const rawId = row?.[idKey];
            const sessionId = Number(row?.session_id);
            if (!Number.isFinite(sessionId)) {
                return;
            }

            const sessionIndex = sessionIndexMap.get(sessionId);
            if (sessionIndex === undefined) {
                return;
            }

            const identifier = (rawId === null || rawId === undefined || rawId === '') ? 'Unknown' : rawId;
            if (!dataByIdentifier.has(identifier)) {
                const resolvedLabel = labelResolver ? labelResolver(identifier) : identifier;
                dataByIdentifier.set(identifier, {
                    id: identifier,
                    label: resolvedLabel || (idKey === 'therapist_id' ? 'Unknown Therapist' : 'Unknown Subtype'),
                    colorId: idKey === 'therapist_id' ? identifier : null,
                    [NEQ_METRIC_SEVERITY]: new Array(totalSessions).fill(null),
                    [NEQ_METRIC_EXPERIENCED]: new Array(totalSessions).fill(null),
                    [NEQ_METRIC_TREATMENT]: new Array(totalSessions).fill(null),
                    [NEQ_METRIC_OTHER]: new Array(totalSessions).fill(null)
                });
            }

            const entry = dataByIdentifier.get(identifier);
            entry[NEQ_METRIC_SEVERITY][sessionIndex] = toNumericOrNull(row?.avg_neq_severity);
            entry[NEQ_METRIC_EXPERIENCED][sessionIndex] = toNumericOrNull(row?.avg_effects_experienced);
            entry[NEQ_METRIC_TREATMENT][sessionIndex] = toNumericOrNull(row?.avg_due_to_treatment);
            entry[NEQ_METRIC_OTHER][sessionIndex] = toNumericOrNull(row?.avg_due_to_other);
        });

        const datasetsByMetric = {
            [NEQ_METRIC_SEVERITY]: [],
            [NEQ_METRIC_EXPERIENCED]: [],
            [NEQ_METRIC_TREATMENT]: [],
            [NEQ_METRIC_OTHER]: []
        };

        let index = 0;
        dataByIdentifier.forEach(entry => {
            NEQ_METRICS.forEach(metric => {
                datasetsByMetric[metric].push({
                    id: entry.id,
                    label: entry.label,
                    data: entry[metric].slice(),
                    index,
                    colorId: entry.colorId || entry.id
                });
            });
            index += 1;
        });

        return {
            sessions,
            labels: sessionLabels,
            datasets: datasetsByMetric
        };
    }

    function updateNeqSessionTrends(query) {
        beginLoading();
        fetch(`/api/neq-session-trends?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load NEQ session trends');
                }
                return res.json();
            })
            .then(apiData => {
                const therapistRecords = apiData && Array.isArray(apiData.therapist) ? apiData.therapist : [];
                const patientRecords = apiData && Array.isArray(apiData.patient) ? apiData.patient : [];

                neqTrendCache[NEQ_VIEW_THERAPIST] = transformNeqTrendRecords(
                    therapistRecords,
                    'therapist_id',
                    id => getTherapistDisplayName(id) || 'Unknown Therapist'
                );

                neqTrendCache[NEQ_VIEW_PATIENT] = transformNeqTrendRecords(
                    patientRecords,
                    'subtype_name',
                    subtype => {
                        if (!subtype || subtype === 'Unknown') {
                            return 'Unknown Subtype';
                        }
                        return subtype;
                    }
                );

                if (currentNeqMode === NEQ_MODE_SESSION) {
                    renderNeqUnifiedChart();
                }
            })
            .catch(error => console.error('Error fetching NEQ session trends:', error))
            .finally(() => endLoading());
    }

    function updateNeqQuestionTable(query) {
        if (!neqTableBody) {
            return;
        }

        beginLoading();
        fetch(`/api/neq-question-summary?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load NEQ question summary');
                }
                return res.json();
            })
            .then(data => {
                neqTableBody.innerHTML = '';

                if (!Array.isArray(data) || data.length === 0) {
                    const emptyRow = document.createElement('tr');
                    emptyRow.innerHTML = '<td colspan="6" class="text-center text-muted">No NEQ responses found for the current filters.</td>';
                    neqTableBody.appendChild(emptyRow);
                    return;
                }

                data.forEach(item => {
                    const questionNumber = Number(item.question_number);
                    const questionText = escapeHtml(item.question_label || `Question ${questionNumber}`);
                    const questionCell = Number.isFinite(questionNumber)
                        ? `Q${questionNumber}. ${questionText}`
                        : questionText;
                    const totalResponses = Number(item.total_responses || 0);
                    const experiencedCount = Number(item.experienced_count || 0);

                    const experiencedCell = totalResponses > 0
                        ? `${formatPercentage(item.experienced_percentage, 1)} <span class="text-muted small">(${experiencedCount}/${totalResponses})</span>`
                        : '—';

                    const severityCell = experiencedCount > 0
                        ? formatSeverity(item.average_severity)
                        : '—';

                    const treatmentCell = experiencedCount > 0
                        ? formatPercentage(item.treatment_percentage, 1)
                        : '—';

                    const otherCell = experiencedCount > 0
                        ? formatPercentage(item.other_percentage, 1)
                        : '—';

                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <th scope="row">${questionCell}</th>
                        <td>${experiencedCell}</td>
                        <td>${severityCell}</td>
                        <td>${treatmentCell}</td>
                        <td>${otherCell}</td>
                    `;
                    neqTableBody.appendChild(row);
                });
            })
            .catch(error => console.error('Error fetching NEQ question summary:', error))
            .finally(() => endLoading());
    }

    function updateSureSessionTrends(query) {
        beginLoading();
        fetch(`/api/sure-session-trends?${query}`)
            .then(res => {
                if (!res.ok) throw new Error('Failed to load SURE session trends');
                return res.json();
            })
            .then(data => {
                const therapistRecords = data && Array.isArray(data.therapist) ? data.therapist : [];
                const patientRecords = data && Array.isArray(data.patient) ? data.patient : [];

                const therapistTransformed = transformSureTrendRecords(
                    therapistRecords,
                    'therapist_id',
                    getTherapistDisplayName
                );

                const patientTransformed = transformSureTrendRecords(
                    patientRecords,
                    'subtype_name',
                    label => label
                );

                sureTrendCache[SURE_VIEW_THERAPIST] = therapistTransformed;
                sureTrendCache[SURE_VIEW_PATIENT] = patientTransformed;

                renderSureOutcomeChart();
            })
            .catch(error => {
                console.error('Error fetching SURE session trends:', error);
                sureTrendCache[SURE_VIEW_THERAPIST] = createEmptySureTrendCache();
                sureTrendCache[SURE_VIEW_PATIENT] = createEmptySureTrendCache();
                renderSureOutcomeChart();
            })
            .finally(() => endLoading());
    }

    function updateSureDomainTrendData(query) {
        beginLoading();
        fetch(`/api/sure-domain-session-trends?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load SURE domain session trends');
                }
                return res.json();
            })
            .then(data => {
                const therapistRecords = data && Array.isArray(data.therapist) ? data.therapist : [];
                const patientRecords = data && Array.isArray(data.patient) ? data.patient : [];

                const therapistTransformed = transformSureDomainTrendRecords(
                    therapistRecords,
                    'therapist_id',
                    getTherapistDisplayName
                );

                const patientTransformed = transformSureDomainTrendRecords(
                    patientRecords,
                    'subtype_name',
                    label => label || 'Unknown'
                );

                sureDomainTrendCache[SURE_VIEW_THERAPIST] = therapistTransformed;
                sureDomainTrendCache[SURE_VIEW_PATIENT] = patientTransformed;

                renderSureOutcomeChart();
            })
            .catch(error => {
                console.error('Error fetching SURE domain session trends:', error);
                sureDomainTrendCache[SURE_VIEW_THERAPIST] = createEmptySureDomainTrendCache();
                sureDomainTrendCache[SURE_VIEW_PATIENT] = createEmptySureDomainTrendCache();
                renderSureOutcomeChart();
            })
            .finally(() => endLoading());
    }

    function updateTherapistSureChart(query) {
        beginLoading();
        fetch(`/api/therapist-comparison-sure?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load SURE comparison data');
                }
                return res.json();
            })
            .then(apiData => {
                const toNumberOrNull = value => {
                    if (value === null || value === undefined) {
                        return null;
                    }
                    const numeric = Number(value);
                    return Number.isFinite(numeric) ? numeric : null;
                };

                const sureData = Array.isArray(apiData) ? apiData : [];
                const rawLabels = sureData.map(d => getTherapistDisplayName(d.therapist_id));
                const sessionCounts = sureData.map(d => Number(d.session_count) || 0);
                const displayLabels = rawLabels.map((label, index) => formatSessionsLabel(label, sessionCounts[index]));

                sureCompositeCache[SURE_VIEW_THERAPIST] = {
                    ids: sureData.map(d => d.therapist_id),
                    labels: displayLabels.slice(),
                    rawLabels: rawLabels.slice(),
                    sessionCounts: sessionCounts.slice(),
                    values: sureData.map(d => toNumberOrNull(d.avg_sure_score))
                };

                renderSureOutcomeChart();
            })
            .catch(error => console.error('Error fetching SURE comparison:', error))
            .finally(() => endLoading());
    }

    function updateTherapistWaiChart(query) {
        beginLoading();
        fetch(`/api/therapist-comparison-wai?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load WAI comparison data');
                }
                return res.json();
            })
            .then(apiData => {
                const filteredData = (apiData || []).filter(d => d.therapist_id !== 'therapist_psych_material');
                const rawLabels = filteredData.map(d => getTherapistDisplayName(d.therapist_id));
                const sessionCounts = filteredData.map(d => Number(d.session_count) || 0);
                const labels = rawLabels.map((label, index) => formatSessionsLabel(label, sessionCounts[index]));
                const toNumberOrNull = value => (value === null || value === undefined ? null : Number(value));
                const values = filteredData.map(d => toNumberOrNull(d.avg_wai_score));
                const taskValues = filteredData.map(d => toNumberOrNull(d.avg_wai_task));
                const bondValues = filteredData.map(d => toNumberOrNull(d.avg_wai_bond));
                const goalValues = filteredData.map(d => toNumberOrNull(d.avg_wai_goal));

                waiComponentCache[WAI_ENTITY_THERAPIST] = {
                    ids: filteredData.map(d => d.therapist_id),
                    labels: labels.slice(),
                    rawLabels: rawLabels.slice(),
                    sessionCounts: sessionCounts.slice(),
                    [WAI_METRIC_TASK]: taskValues.map(value => (value === null || value === undefined) ? null : Number(value)),
                    [WAI_METRIC_BOND]: bondValues.map(value => (value === null || value === undefined) ? null : Number(value)),
                    [WAI_METRIC_GOAL]: goalValues.map(value => (value === null || value === undefined) ? null : Number(value))
                };

                waiCompositeCache[WAI_ENTITY_THERAPIST] = {
                    ids: filteredData.map(d => d.therapist_id),
                    labels: labels.slice(),
                    rawLabels: rawLabels.slice(),
                    sessionCounts: sessionCounts.slice(),
                    values: values.map(value => (value === null || value === undefined) ? null : Number(value))
                };

                renderWaiUnifiedChart();
            })
            .catch(error => console.error('Error fetching WAI comparison:', error))
            .finally(() => endLoading());
    }

    function getSrsMetricLabel(metric) {
        return SRS_METRIC_LABELS[metric] || 'SRS Score';
    }

    function updateSrsOutcomeToggleStyles() {
        if (srsOutcomeViewToggleButtons.length) {
            srsOutcomeViewToggleButtons.forEach(button => {
                const viewValue = button.dataset.srsOutcomeView;
                const isActive = viewValue === currentSrsOutcomeView;
                button.classList.toggle('active', isActive);
                if (isActive) {
                    button.classList.add('btn-primary');
                    button.classList.remove('btn-outline-primary');
                } else {
                    button.classList.remove('btn-primary');
                    button.classList.add('btn-outline-primary');
                }
            });
        }

        if (srsOutcomeLevelToggleButtons.length) {
            srsOutcomeLevelToggleButtons.forEach(button => {
                const levelValue = button.dataset.srsOutcomeLevel;
                const isActive = levelValue === currentSrsOutcomeLevel;
                button.classList.toggle('active', isActive);
                if (isActive) {
                    button.classList.add('btn-primary');
                    button.classList.remove('btn-outline-primary');
                } else {
                    button.classList.remove('btn-primary');
                    button.classList.add('btn-outline-primary');
                }
            });
        }

        if (srsOutcomeMetricToggleButtons.length) {
            srsOutcomeMetricToggleButtons.forEach(button => {
                const metricValue = button.dataset.srsOutcomeMetric;
                const isActive = metricValue === currentSrsOutcomeMetric;
                button.classList.toggle('active', isActive);
                if (isActive) {
                    button.classList.add('btn-secondary');
                    button.classList.remove('btn-outline-secondary');
                } else {
                    button.classList.remove('btn-secondary');
                    button.classList.add('btn-outline-secondary');
                }
            });
        }
    }

    function renderSrsOutcomeChart() {
        updateSrsOutcomeToggleStyles();

        const metricConfig = SRS_OUTCOME_METRIC_CONFIG[currentSrsOutcomeMetric]
            || SRS_OUTCOME_METRIC_CONFIG[SRS_OUTCOME_METRIC_TOTAL];
        const axisConfig = metricConfig.axis || { min: 0, max: 10, step: 1 };
        const viewLabelSuffix = currentSrsOutcomeView === SRS_VIEW_THERAPIST
            ? 'Therapist Average'
            : 'Patient Subtype Average';

        if (currentSrsOutcomeLevel === SRS_OUTCOME_LEVEL_INTERVENTION) {
            let labels = [];
            let values = [];
            let ids = [];

            if (currentSrsOutcomeMetric === SRS_OUTCOME_METRIC_TOTAL) {
                const cache = srsCompositeCache[currentSrsOutcomeView] || { ids: [], labels: [], values: [] };
                labels = Array.isArray(cache.labels) ? cache.labels.slice() : [];
                if (!labels.length && Array.isArray(cache.rawLabels)) {
                    labels = cache.rawLabels.slice();
                }
                values = Array.isArray(cache.values) ? cache.values.map(toNumericOrNull) : [];
                ids = Array.isArray(cache.ids) ? cache.ids.slice() : labels.slice();
            } else {
                const metricKey = metricConfig.srsMetricKey;
                const cache = srsComparisonCache[currentSrsOutcomeView] || {};
                labels = Array.isArray(cache.labels) ? cache.labels.slice() : [];
                if (!labels.length && Array.isArray(cache.rawLabels)) {
                    labels = cache.rawLabels.slice();
                }
                const metricValues = metricKey && Array.isArray(cache[metricKey]) ? cache[metricKey] : [];
                values = metricValues.map(toNumericOrNull);
                ids = Array.isArray(cache.ids) ? cache.ids.slice() : labels.slice();
            }

            const colorMetricKey = metricConfig.srsMetricKey || SRS_METRIC_OVERALL;
            const patientColorConfig = SRS_PATIENT_COLORS[colorMetricKey] || PATIENT_VIEW_COLORS;

            if (!labels.length) {
                createOrUpdateChart('srsOutcomeChart', 'bar', {
                    labels: [],
                    datasets: []
                }, {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: {
                            min: axisConfig.min,
                            max: axisConfig.max,
                            title: { display: true, text: metricConfig.label },
                            ticks: {
                                stepSize: axisConfig.step,
                                callback: value => `${value}`
                            }
                        }
                    },
                    plugins: { legend: { display: false } }
                });
                return;
            }

            const backgroundColors = labels.map((label, index) => {
                if (currentSrsOutcomeView === SRS_VIEW_THERAPIST) {
                    const color = getTherapistColors(ids[index] || label);
                    return color.background;
                }
                return patientColorConfig.background;
            });

            const borderColors = labels.map((label, index) => {
                if (currentSrsOutcomeView === SRS_VIEW_THERAPIST) {
                    const color = getTherapistColors(ids[index] || label);
                    return color.border;
                }
                return patientColorConfig.border;
            });

            const datasetLabel = `${metricConfig.label} (${viewLabelSuffix})`;

            createOrUpdateChart('srsOutcomeChart', 'bar', {
                labels,
                datasets: [{
                    label: datasetLabel,
                    data: values,
                    backgroundColor: backgroundColors,
                    borderColor: borderColors,
                    borderWidth: 1
                }]
            }, {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        min: axisConfig.min,
                        max: axisConfig.max,
                        title: { display: true, text: metricConfig.label },
                        ticks: {
                            stepSize: axisConfig.step,
                            callback: value => `${value}`
                        }
                    }
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: context => {
                                const value = context.parsed.x;
                                if (value === null || value === undefined || Number.isNaN(value)) {
                                    return `${metricConfig.label}: No data`;
                                }
                                return `${metricConfig.label}: ${Number(value).toFixed(2)}`;
                            }
                        }
                    }
                }
            });
            return;
        }

        const yAxisOptions = {
            min: axisConfig.min,
            max: axisConfig.max,
            title: { display: true, text: metricConfig.label },
            ticks: {
                stepSize: axisConfig.step,
                callback: value => `${value}`
            }
        };

        if (currentSrsOutcomeMetric === SRS_OUTCOME_METRIC_TOTAL) {
            const cache = srsCompositeTrendCache[currentSrsOutcomeView] || createEmptySureTrendCache();
            const sessionLabels = Array.isArray(cache.labels) ? cache.labels.slice() : [];
            const datasets = Array.isArray(cache.datasets) ? cache.datasets : [];

            const chartDatasets = datasets.map(series => {
                const colors = currentSrsOutcomeView === SRS_VIEW_THERAPIST
                    ? getTherapistColors(series.id)
                    : getPatientTrendColor(series.index);

                return {
                    label: series.label,
                    data: Array.isArray(series.data) ? series.data.slice() : [],
                    borderColor: colors.border,
                    backgroundColor: colors.background,
                    tension: 0.3,
                    fill: false,
                    spanGaps: true,
                    borderWidth: 2,
                    pointRadius: 3,
                    pointHoverRadius: 4
                };
            });

            createOrUpdateChart('srsOutcomeChart', 'line', {
                labels: sessionLabels,
                datasets: chartDatasets
            }, {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: { title: { display: true, text: 'Session' } },
                    y: yAxisOptions
                },
                plugins: {
                    legend: { position: 'bottom', labels: { boxWidth: 12 } },
                    tooltip: {
                        callbacks: {
                            label: context => {
                                const value = context.parsed.y;
                                if (value === null || value === undefined || Number.isNaN(value)) {
                                    return `${context.dataset.label}: No data`;
                                }
                                return `${context.dataset.label}: ${Number(value).toFixed(2)}`;
                            }
                        }
                    }
                }
            });
            return;
        }

        const metricKey = metricConfig.srsMetricKey;
        const trendCache = srsTrendCache[currentSrsOutcomeView] || createEmptySrsTrendCache();
        const sessionLabels = Array.isArray(trendCache.labels) ? trendCache.labels.slice() : [];
        const metricDatasets = metricKey && trendCache.datasets && Array.isArray(trendCache.datasets[metricKey])
            ? trendCache.datasets[metricKey]
            : [];

        const chartDatasets = metricDatasets.map(series => {
            const colors = currentSrsOutcomeView === SRS_VIEW_THERAPIST
                ? getTherapistColors(series.id)
                : getPatientTrendColor(series.index);

            return {
                label: series.label,
                data: Array.isArray(series.data) ? series.data.slice() : [],
                borderColor: colors.border,
                backgroundColor: colors.background,
                tension: 0.3,
                fill: false,
                spanGaps: true,
                borderWidth: 2,
                pointRadius: 3,
                pointHoverRadius: 4
            };
        });

        createOrUpdateChart('srsOutcomeChart', 'line', {
            labels: sessionLabels,
            datasets: chartDatasets
        }, {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { title: { display: true, text: 'Session' } },
                y: yAxisOptions
            },
            plugins: {
                legend: { position: 'bottom', labels: { boxWidth: 12 } },
                tooltip: {
                    callbacks: {
                        label: context => {
                            const value = context.parsed.y;
                            if (value === null || value === undefined || Number.isNaN(value)) {
                                return `${context.dataset.label}: No data`;
                            }
                            return `${context.dataset.label}: ${Number(value).toFixed(2)}`;
                        }
                    }
                }
            }
        });
    }

    function getWaiMetricLabel(metric) {
        return WAI_METRIC_LABELS[metric] || 'WAI Score';
    }

    function updateWaiToggleStyles() {
        if (waiModeToggleButtons.length) {
            waiModeToggleButtons.forEach(button => {
                const modeValue = button.dataset.waiMode;
                const isActive = modeValue === currentWaiMode;
                button.classList.toggle('active', isActive);
                button.classList.toggle('btn-primary', isActive);
                button.classList.toggle('btn-outline-primary', !isActive);
            });
        }

        if (waiEntityToggleButtons.length) {
            waiEntityToggleButtons.forEach(button => {
                const entityValue = button.dataset.waiEntity;
                const isActive = entityValue === currentWaiEntity;
                button.classList.toggle('active', isActive);
                button.classList.toggle('btn-primary', isActive);
                button.classList.toggle('btn-outline-primary', !isActive);
            });
        }

        if (waiMetricToggleButtons.length) {
            waiMetricToggleButtons.forEach(button => {
                const metricValue = button.dataset.waiMetric;
                const isActive = metricValue === currentWaiMetric;
                button.classList.toggle('active', isActive);
                button.classList.toggle('btn-secondary', isActive);
                button.classList.toggle('btn-outline-secondary', !isActive);
            });
        }
    }

    function renderWaiUnifiedChart() {
        updateWaiToggleStyles();

        const sessionMode = currentWaiMode === WAI_MODE_SESSION;
        const totalMetric = currentWaiMetric === WAI_METRIC_TOTAL;
        const metricLabel = getWaiMetricLabel(currentWaiMetric);

        if (!sessionMode) {
            let labels = [];
            let values = [];
            let identifiers = [];

            if (totalMetric) {
                const viewData = waiCompositeCache[currentWaiEntity] || { ids: [], labels: [], values: [] };
                labels = Array.isArray(viewData.labels) ? viewData.labels.slice() : [];
                if (!labels.length && Array.isArray(viewData.rawLabels)) {
                    labels = viewData.rawLabels.slice();
                }
                values = Array.isArray(viewData.values) ? viewData.values.map(toNumericOrNull) : [];
                identifiers = currentWaiEntity === WAI_ENTITY_THERAPIST
                    ? (Array.isArray(viewData.ids) ? viewData.ids.slice() : [])
                    : labels.map((_, index) => `patient_${index}`);
            } else {
                const viewData = waiComponentCache[currentWaiEntity] || {
                    ids: [],
                    labels: [],
                    [WAI_METRIC_TASK]: [],
                    [WAI_METRIC_BOND]: [],
                    [WAI_METRIC_GOAL]: []
                };
                labels = Array.isArray(viewData.labels) ? viewData.labels.slice() : [];
                if (!labels.length && Array.isArray(viewData.rawLabels)) {
                    labels = viewData.rawLabels.slice();
                }
                values = Array.isArray(viewData[currentWaiMetric])
                    ? viewData[currentWaiMetric].map(toNumericOrNull)
                    : [];
                identifiers = currentWaiEntity === WAI_ENTITY_THERAPIST
                    ? (Array.isArray(viewData.ids) ? viewData.ids.slice() : [])
                    : labels.map((_, index) => `patient_${index}`);
            }

            const colors = labels.map((_, index) => (
                currentWaiEntity === WAI_ENTITY_THERAPIST
                    ? getTherapistColors(identifiers[index])
                    : getPatientTrendColor(index)
            ));

            const axisConfig = totalMetric
                ? { min: 36, max: 252, step: 36 }
                : { min: 12, max: 84, step: 12 };

            createOrUpdateChart('waiOutcomesChart', 'bar', {
                labels,
                datasets: [{
                    label: `${metricLabel} (${currentWaiEntity === WAI_ENTITY_THERAPIST ? 'Therapist Average' : 'Patient Subtype Average'})`,
                    data: values,
                    backgroundColor: colors.map(color => color.background),
                    borderColor: colors.map(color => color.border),
                    borderWidth: 1
                }]
            }, {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        min: axisConfig.min,
                        max: axisConfig.max,
                        title: { display: true, text: metricLabel },
                        ticks: {
                            stepSize: axisConfig.step,
                            callback: value => `${value}`
                        }
                    }
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: context => {
                                const value = context.parsed.x;
                                if (value === null || value === undefined || Number.isNaN(value)) {
                                    return 'No data';
                                }
                                return `${metricLabel}: ${Number(value).toFixed(1)}`;
                            }
                        }
                    }
                }
            });
            return;
        }

        let labels = [];
        let datasetDefs = [];

        if (totalMetric) {
            const cache = waiCompositeTrendCache[currentWaiEntity] || createEmptySureTrendCache();
            labels = Array.isArray(cache.labels) ? cache.labels.slice() : [];
            datasetDefs = Array.isArray(cache.datasets) ? cache.datasets : [];
        } else {
            const cache = waiTrendCache[currentWaiEntity] || createEmptyTrendCache();
            labels = Array.isArray(cache.labels) ? cache.labels.slice() : [];
            datasetDefs = cache.datasets?.[currentWaiMetric] || [];
        }

        const datasets = datasetDefs.map(series => {
            const palette = currentWaiEntity === WAI_ENTITY_THERAPIST
                ? getTherapistColors(series.id)
                : getPatientTrendColor(series.index);
            return {
                label: series.label,
                data: Array.isArray(series.data) ? series.data.slice() : [],
                borderColor: palette.border,
                backgroundColor: palette.background,
                tension: 0.3,
                fill: false,
                spanGaps: true,
                borderWidth: 2,
                pointRadius: 3,
                pointHoverRadius: 4
            };
        });

        const yAxis = totalMetric
            ? {
                min: 36,
                max: 252,
                title: { display: true, text: metricLabel },
                ticks: {
                    stepSize: 36,
                    callback: value => `${value}`
                }
            }
            : {
                suggestedMin: 12,
                suggestedMax: 84,
                title: { display: true, text: metricLabel },
                ticks: {
                    stepSize: 12,
                    callback: value => `${value}`
                }
            };

        createOrUpdateChart('waiOutcomesChart', 'line', {
            labels,
            datasets
        }, {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { title: { display: true, text: 'Session' } },
                y: yAxis
            },
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { boxWidth: 12 }
                },
                tooltip: {
                    callbacks: {
                        label: context => {
                            const value = context.parsed.y;
                            if (value === null || value === undefined || Number.isNaN(value)) {
                                return `${context.dataset.label}: No data`;
                            }
                            return `${context.dataset.label}: ${Number(value).toFixed(1)}`;
                        }
                    }
                }
            }
        });
    }

    function getPatientTrendColor(index) {
        return PATIENT_TREND_COLOR_PALETTE[index % PATIENT_TREND_COLOR_PALETTE.length];
    }

    function getPatientBarColors(index) {
        const palette = getPatientTrendColor(index);
        const border = palette.border;
        if (typeof border === 'string' && border.startsWith('rgba')) {
            const background = border.replace(/rgba\(([^)]+),\s*1\)/, 'rgba($1, 0.7)');
            return { border, background };
        }
        return palette;
    }

    function toNumericOrNull(value) {
        if (value === null || value === undefined) {
            return null;
        }
        const numeric = Number(value);
        return Number.isFinite(numeric) ? numeric : null;
    }

    function calculateMedian(values) {
        if (!Array.isArray(values) || values.length === 0) {
            return null;
        }
        const numeric = values.filter(value => Number.isFinite(value));
        if (!numeric.length) {
            return null;
        }
        const sorted = numeric.slice().sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        if (sorted.length % 2 === 0) {
            return (sorted[mid - 1] + sorted[mid]) / 2;
        }
        return sorted[mid];
    }

    function formatMedianLabel(value) {
        if (!Number.isFinite(value)) {
            return '';
        }
        return Number.isInteger(value) ? value.toString() : value.toFixed(2);
    }

    function calculateQuartileSummary(values) {
        if (!Array.isArray(values) || values.length === 0) {
            return null;
        }
        const numeric = values.filter(value => Number.isFinite(value)).sort((a, b) => a - b);
        if (!numeric.length) {
            return null;
        }

        const median = calculateMedian(numeric);
        const mid = Math.floor(numeric.length / 2);
        const lowerSlice = numeric.slice(0, mid);
        const upperSlice = numeric.slice(numeric.length % 2 === 0 ? mid : mid + 1);

        const q1 = calculateMedian(lowerSlice.length ? lowerSlice : [numeric[0]]);
        const q3 = calculateMedian(upperSlice.length ? upperSlice : [numeric[numeric.length - 1]]);

        return {
            min: numeric[0],
            q1,
            median,
            q3,
            max: numeric[numeric.length - 1],
            values: numeric
        };
    }

    function ensureNeqRecord(view, key, label, colorId) {
        const cache = neqInterventionCache[view];
        if (!cache) {
            return null;
        }

        const normalizedKey = (key === null || key === undefined || key === '') ? 'Unknown' : key;
        if (!cache.has(normalizedKey)) {
            cache.set(normalizedKey, {
                key: normalizedKey,
                label: label || (view === NEQ_VIEW_THERAPIST ? 'Unknown Therapist' : 'Unknown Subtype'),
                colorId: colorId || (view === NEQ_VIEW_THERAPIST ? normalizedKey : null),
                [NEQ_METRIC_SEVERITY]: null,
                [NEQ_METRIC_EXPERIENCED]: null,
                [NEQ_METRIC_TREATMENT]: null,
                [NEQ_METRIC_OTHER]: null
            });
        }

        const record = cache.get(normalizedKey);
        if (label && label !== record.label) {
            record.label = label;
        }
        if (colorId) {
            record.colorId = colorId;
        }
        if (!neqInterventionOrder[view].includes(normalizedKey)) {
            neqInterventionOrder[view].push(normalizedKey);
        }

        return record;
    }

    function resetNeqCaches() {
        Object.values(neqInterventionCache).forEach(cache => {
            if (cache && typeof cache.clear === 'function') {
                cache.clear();
            }
        });

        Object.keys(neqInterventionOrder).forEach(view => {
            neqInterventionOrder[view] = [];
        });

        Object.keys(neqTrendCache).forEach(view => {
            neqTrendCache[view] = createEmptyNeqTrendCache();
        });
    }

    function setNeqMetric(view, key, label, metric, rawValue, colorId) {
        if (!NEQ_METRICS.includes(metric)) {
            return;
        }
        const record = ensureNeqRecord(view, key, label, colorId);
        if (!record) {
            return;
        }
        record[metric] = toNumericOrNull(rawValue);
    }

    function transformSrsTrendRecords(records, idKey, labelResolver) {
        const sessionNumbers = Array.from(new Set((records || [])
            .map(row => Number(row.session_id))
            .filter(Number.isFinite))).sort((a, b) => a - b);

        const sessionLabels = sessionNumbers.map(session => `Session ${session}`);
        const sessionIndexMap = new Map();
        sessionNumbers.forEach((session, index) => {
            sessionIndexMap.set(session, index);
        });

        const dataById = new Map();
        const totalSessions = sessionNumbers.length;

        (records || []).forEach(row => {
            const idValue = row[idKey];
            const sessionId = Number(row.session_id);
            if (!Number.isFinite(sessionId)) {
                return;
            }
            const sessionIndex = sessionIndexMap.get(sessionId);
            if (sessionIndex === undefined) {
                return;
            }

            const identifier = idValue || (idKey === 'therapist_id' ? 'unknown_therapist' : 'Unknown');

            if (!dataById.has(identifier)) {
                dataById.set(identifier, {
                    id: identifier,
                    label: labelResolver(identifier),
                    [SRS_METRIC_OVERALL]: new Array(totalSessions).fill(null),
                    [SRS_METRIC_RELATIONSHIP]: new Array(totalSessions).fill(null),
                    [SRS_METRIC_GOALS]: new Array(totalSessions).fill(null),
                    [SRS_METRIC_APPROACH]: new Array(totalSessions).fill(null)
                });
            }

            const entry = dataById.get(identifier);
            entry[SRS_METRIC_OVERALL][sessionIndex] = toNumericOrNull(row.avg_srs_overall);
            entry[SRS_METRIC_RELATIONSHIP][sessionIndex] = toNumericOrNull(row.avg_srs_relationship);
            entry[SRS_METRIC_GOALS][sessionIndex] = toNumericOrNull(row.avg_srs_goals);
            entry[SRS_METRIC_APPROACH][sessionIndex] = toNumericOrNull(row.avg_srs_approach);
        });

        const datasetsByMetric = {
            [SRS_METRIC_OVERALL]: [],
            [SRS_METRIC_RELATIONSHIP]: [],
            [SRS_METRIC_GOALS]: [],
            [SRS_METRIC_APPROACH]: []
        };

        let index = 0;
        dataById.forEach(entry => {
            [SRS_METRIC_OVERALL, SRS_METRIC_RELATIONSHIP, SRS_METRIC_GOALS, SRS_METRIC_APPROACH].forEach(metric => {
                datasetsByMetric[metric].push({
                    id: entry.id,
                    label: entry.label,
                    data: entry[metric].slice(),
                    index
                });
            });
            index += 1;
        });

        return {
            sessions: sessionNumbers,
            labels: sessionLabels,
            datasets: datasetsByMetric
        };
    }

    function getSureOutcomeMetricConfig(metric) {
        return SURE_OUTCOME_METRIC_CONFIG[metric] || SURE_OUTCOME_METRIC_CONFIG[SURE_OUTCOME_METRIC_TOTAL];
    }

    function updateSureOutcomeToggleStyles() {
        if (sureOutcomeLevelToggleButtons.length) {
            sureOutcomeLevelToggleButtons.forEach(button => {
                const levelValue = button.dataset.sureOutcomeLevel;
                const isActive = levelValue === currentSureOutcomeLevel;
                button.classList.toggle('active', isActive);
                if (isActive) {
                    button.classList.add('btn-primary');
                    button.classList.remove('btn-outline-primary');
                } else {
                    button.classList.remove('btn-primary');
                    button.classList.add('btn-outline-primary');
                }
            });
        }

        if (sureOutcomeViewToggleButtons.length) {
            sureOutcomeViewToggleButtons.forEach(button => {
                const viewValue = button.dataset.sureOutcomeView;
                const isActive = viewValue === currentSureOutcomeView;
                button.classList.toggle('active', isActive);
                if (isActive) {
                    button.classList.add('btn-primary');
                    button.classList.remove('btn-outline-primary');
                } else {
                    button.classList.remove('btn-primary');
                    button.classList.add('btn-outline-primary');
                }
            });
        }

        if (sureOutcomeMetricToggleButtons.length) {
            sureOutcomeMetricToggleButtons.forEach(button => {
                const metricValue = button.dataset.sureOutcomeMetric;
                const isActive = metricValue === currentSureOutcomeMetric;
                button.classList.toggle('active', isActive);
                if (isActive) {
                    button.classList.add('btn-secondary');
                    button.classList.remove('btn-outline-secondary');
                } else {
                    button.classList.remove('btn-secondary');
                    button.classList.add('btn-outline-secondary');
                }
            });
        }
    }

    function renderSureOutcomeChart() {
        updateSureOutcomeToggleStyles();

        const metricConfig = getSureOutcomeMetricConfig(currentSureOutcomeMetric);
        const viewLabelSuffix = currentSureOutcomeView === SURE_VIEW_THERAPIST
            ? 'Therapist Average'
            : 'Patient Subtype Average';

        if (currentSureOutcomeLevel === SURE_OUTCOME_LEVEL_INTERVENTION) {
            let labels = [];
            let ids = [];
            let values = [];

            if (currentSureOutcomeMetric === SURE_OUTCOME_METRIC_TOTAL) {
                const cache = sureCompositeCache[currentSureOutcomeView] || { ids: [], labels: [], values: [] };
                labels = Array.isArray(cache.labels) ? cache.labels.slice() : [];
                if (!labels.length && Array.isArray(cache.rawLabels)) {
                    labels = cache.rawLabels.slice();
                }
                ids = Array.isArray(cache.ids) ? cache.ids.slice() : labels.map((_, index) => index);
                values = Array.isArray(cache.values) ? cache.values.map(toNumericOrNull) : [];
            } else {
                const viewData = sureDomainAggregateCache[currentSureOutcomeView] || {
                    ids: [],
                    labels: []
                };
                labels = Array.isArray(viewData.labels) ? viewData.labels.slice() : [];
                if (!labels.length && Array.isArray(viewData.rawLabels)) {
                    labels = viewData.rawLabels.slice();
                }
                const columnKey = SURE_DOMAIN_METRIC_CONFIG[currentSureOutcomeMetric]?.column;
                ids = Array.isArray(viewData.ids) ? viewData.ids.slice() : labels.map((_, index) => index);
                values = columnKey && Array.isArray(viewData[columnKey])
                    ? viewData[columnKey].map(toNumericOrNull)
                    : new Array(labels.length).fill(null);
            }

            const backgroundColors = labels.map((_, index) => {
                if (currentSureOutcomeView === SURE_VIEW_THERAPIST) {
                    return getTherapistColors(ids[index]).background;
                }
                return getPatientTrendColor(index).background;
            });

            const borderColors = labels.map((_, index) => {
                if (currentSureOutcomeView === SURE_VIEW_THERAPIST) {
                    return getTherapistColors(ids[index]).border;
                }
                return getPatientTrendColor(index).border;
            });

            const numericValues = values.filter(value => Number.isFinite(value));
            const fallbackSuggestedMax = numericValues.length
                ? Math.ceil(Math.max(...numericValues) / 5) * 5
                : 5;

            const datasetLabel = `${metricConfig.label} (${viewLabelSuffix})`;
            const hasData = labels.length > 0 && numericValues.length > 0;

            createOrUpdateChart('sureOutcomesChart', 'bar', {
                labels,
                datasets: hasData ? [{
                    label: datasetLabel,
                    data: values,
                    backgroundColor: backgroundColors,
                    borderColor: borderColors,
                    borderWidth: 1
                }] : []
            }, {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        ...(metricConfig.range
                            ? { min: metricConfig.range.min, max: metricConfig.range.max }
                            : { beginAtZero: true, suggestedMax: fallbackSuggestedMax }),
                        title: { display: true, text: metricConfig.label },
                        ticks: {
                            callback: value => `${value}`
                        }
                    }
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: context => {
                                const value = context.parsed.x;
                                if (value === null || value === undefined || Number.isNaN(value)) {
                                    return `${datasetLabel}: No data`;
                                }
                                return `${datasetLabel}: ${Number(value).toFixed(2)}`;
                            }
                        }
                    }
                }
            });
            return;
        }

        let labels = [];
        let datasets = [];

        if (currentSureOutcomeMetric === SURE_OUTCOME_METRIC_TOTAL) {
            const cache = sureTrendCache[currentSureOutcomeView] || createEmptySureTrendCache();
            labels = Array.isArray(cache.labels) ? cache.labels.slice() : [];
            const sourceDatasets = Array.isArray(cache.datasets) ? cache.datasets : [];
            datasets = sourceDatasets.map(series => {
                const colors = currentSureOutcomeView === SURE_VIEW_THERAPIST
                    ? getTherapistColors(series.id)
                    : getPatientTrendColor(series.index);
                return {
                    label: series.label,
                    data: Array.isArray(series.data) ? series.data.slice() : [],
                    borderColor: colors.border,
                    backgroundColor: colors.background,
                    tension: 0.3,
                    fill: false,
                    spanGaps: true,
                    borderWidth: 2,
                    pointRadius: 3,
                    pointHoverRadius: 4
                };
            });
        } else {
            const cache = sureDomainTrendCache[currentSureOutcomeView] || createEmptySureDomainTrendCache();
            labels = Array.isArray(cache.labels) ? cache.labels.slice() : [];
            const metricDatasets = cache.datasets?.[currentSureOutcomeMetric] || [];
            datasets = metricDatasets.map(series => {
                const colors = currentSureOutcomeView === SURE_VIEW_THERAPIST
                    ? getTherapistColors(series.id)
                    : getPatientTrendColor(series.index);
                return {
                    label: series.label,
                    data: Array.isArray(series.data) ? series.data.slice() : [],
                    borderColor: colors.border,
                    backgroundColor: colors.background,
                    tension: 0.3,
                    fill: false,
                    spanGaps: true,
                    borderWidth: 2,
                    pointRadius: 3,
                    pointHoverRadius: 4
                };
            });
        }

        createOrUpdateChart('sureOutcomesChart', 'line', {
            labels,
            datasets
        }, {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    title: { display: true, text: 'Session' }
                },
                y: {
                    ...(metricConfig.range ? { min: metricConfig.range.min, max: metricConfig.range.max } : {}),
                    title: { display: true, text: metricConfig.label },
                    ticks: {
                        stepSize: metricConfig.range ? 1 : undefined,
                        callback: value => `${value}`
                    }
                }
            },
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { boxWidth: 12 }
                },
                tooltip: {
                    callbacks: {
                        label: context => {
                            const value = context.parsed.y;
                            if (value === null || value === undefined || Number.isNaN(value)) {
                                return `${context.dataset.label}: No data`;
                            }
                            return `${context.dataset.label}: ${Number(value).toFixed(2)}`;
                        }
                    }
                }
            }
        });
    }

    function updateSureDomainAggregateChartData(query) {
        beginLoading();
        fetch(`/api/sure-domain-aggregates?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load SURE domain aggregates');
                }
                return res.json();
            })
            .then(apiData => {
                const therapistRecords = apiData && Array.isArray(apiData.therapist) ? apiData.therapist : [];
                const patientRecords = apiData && Array.isArray(apiData.patient) ? apiData.patient : [];

                const therapistSessionCounts = therapistRecords.map(row => Number(row.session_count) || 0);
                const patientSessionCounts = patientRecords.map(row => Number(row.session_count) || 0);

                sureDomainAggregateCache[SURE_VIEW_THERAPIST] = {
                    ids: therapistRecords.map(row => row.therapist_id || 'unknown_therapist'),
                    labels: therapistRecords.map((row, index) => {
                        const therapistId = row.therapist_id || 'unknown_therapist';
                        const displayName = getTherapistDisplayName(therapistId);
                        if (!displayName || displayName === therapistId) {
                            const base = therapistId === 'unknown_therapist' ? 'Unknown Therapist' : therapistId;
                            return formatSessionsLabel(base, therapistSessionCounts[index]);
                        }
                        return formatSessionsLabel(displayName, therapistSessionCounts[index]);
                    }),
                    rawLabels: therapistRecords.map(row => {
                        const therapistId = row.therapist_id || 'unknown_therapist';
                        const displayName = getTherapistDisplayName(therapistId);
                        if (!displayName || displayName === therapistId) {
                            return therapistId === 'unknown_therapist' ? 'Unknown Therapist' : therapistId;
                        }
                        return displayName;
                    }),
                    sessionCounts: therapistSessionCounts.slice(),
                    total_sure_drug_use: therapistRecords.map(row => toNumericOrNull(row.total_sure_drug_use)),
                    total_sure_self_care: therapistRecords.map(row => toNumericOrNull(row.total_sure_self_care)),
                    total_sure_relationships: therapistRecords.map(row => toNumericOrNull(row.total_sure_relationships)),
                    total_sure_material_resources: therapistRecords.map(row => toNumericOrNull(row.total_sure_material_resources)),
                    total_sure_outlook: therapistRecords.map(row => toNumericOrNull(row.total_sure_outlook))
                };

                sureDomainAggregateCache[SURE_VIEW_PATIENT] = {
                    ids: patientRecords.map(row => row.subtype_name || 'Unknown'),
                    labels: patientRecords.map((row, index) => {
                        const baseLabel = row.subtype_name && row.subtype_name !== 'Unknown' ? row.subtype_name : 'Unknown Subtype';
                        return formatSessionsLabel(baseLabel, patientSessionCounts[index]);
                    }),
                    rawLabels: patientRecords.map(row => row.subtype_name && row.subtype_name !== 'Unknown' ? row.subtype_name : 'Unknown Subtype'),
                    sessionCounts: patientSessionCounts.slice(),
                    total_sure_drug_use: patientRecords.map(row => toNumericOrNull(row.total_sure_drug_use)),
                    total_sure_self_care: patientRecords.map(row => toNumericOrNull(row.total_sure_self_care)),
                    total_sure_relationships: patientRecords.map(row => toNumericOrNull(row.total_sure_relationships)),
                    total_sure_material_resources: patientRecords.map(row => toNumericOrNull(row.total_sure_material_resources)),
                    total_sure_outlook: patientRecords.map(row => toNumericOrNull(row.total_sure_outlook))
                };

                renderSureOutcomeChart();
            })
            .catch(error => {
                console.error('Error fetching SURE domain aggregates:', error);
                sureDomainAggregateCache[SURE_VIEW_THERAPIST] = {
                    ids: [],
                    labels: [],
                    total_sure_drug_use: [],
                    total_sure_self_care: [],
                    total_sure_relationships: [],
                    total_sure_material_resources: [],
                    total_sure_outlook: []
                };
                sureDomainAggregateCache[SURE_VIEW_PATIENT] = {
                    ids: [],
                    labels: [],
                    total_sure_drug_use: [],
                    total_sure_self_care: [],
                    total_sure_relationships: [],
                    total_sure_material_resources: [],
                    total_sure_outlook: []
                };
                renderSureOutcomeChart();
            })
            .finally(() => endLoading());
    }

    function transformWaiTrendRecords(records, idKey, labelResolver) {
        const sessionNumbers = Array.from(new Set((records || [])
            .map(row => Number(row.session_id))
            .filter(Number.isFinite))).sort((a, b) => a - b);

        const sessionLabels = sessionNumbers.map(session => `Session ${session}`);
        const sessionIndexMap = new Map();
        sessionNumbers.forEach((session, index) => {
            sessionIndexMap.set(session, index);
        });

        const dataById = new Map();
        const totalSessions = sessionNumbers.length;

        (records || []).forEach(row => {
            const idValue = row[idKey];
            const sessionId = Number(row.session_id);
            if (!Number.isFinite(sessionId)) {
                return;
            }
            const sessionIndex = sessionIndexMap.get(sessionId);
            if (sessionIndex === undefined) {
                return;
            }

            const identifier = idValue || (idKey === 'therapist_id' ? 'unknown_therapist' : 'Unknown');

            if (!dataById.has(identifier)) {
                dataById.set(identifier, {
                    id: identifier,
                    label: labelResolver(identifier),
                    [WAI_METRIC_TASK]: new Array(totalSessions).fill(null),
                    [WAI_METRIC_BOND]: new Array(totalSessions).fill(null),
                    [WAI_METRIC_GOAL]: new Array(totalSessions).fill(null)
                });
            }

            const entry = dataById.get(identifier);
            entry[WAI_METRIC_TASK][sessionIndex] = toNumericOrNull(row.avg_wai_task);
            entry[WAI_METRIC_BOND][sessionIndex] = toNumericOrNull(row.avg_wai_bond);
            entry[WAI_METRIC_GOAL][sessionIndex] = toNumericOrNull(row.avg_wai_goal);
        });

        const datasetsByMetric = {
            [WAI_METRIC_TASK]: [],
            [WAI_METRIC_BOND]: [],
            [WAI_METRIC_GOAL]: []
        };

        let index = 0;
        dataById.forEach(entry => {
            [WAI_METRIC_TASK, WAI_METRIC_BOND, WAI_METRIC_GOAL].forEach(metric => {
                datasetsByMetric[metric].push({
                    id: entry.id,
                    label: entry.label,
                    data: entry[metric].slice(),
                    index
                });
            });
            index += 1;
        });

        return {
            sessions: sessionNumbers,
            labels: sessionLabels,
            datasets: datasetsByMetric
        };
    }

    function transformSureTrendRecords(records, idKey, labelResolver, valueKey = 'avg_sure') {
        const sessionNumbers = Array.from(new Set((records || [])
            .map(row => Number(row.session_id))
            .filter(Number.isFinite))).sort((a, b) => a - b);

        const sessionLabels = sessionNumbers.map(session => `Session ${session}`);
        const sessionIndexMap = new Map();
        sessionNumbers.forEach((session, index) => {
            sessionIndexMap.set(session, index);
        });

        const totalSessions = sessionNumbers.length;
        const dataByIdentifier = new Map();

        (records || []).forEach(row => {
            const rawId = row?.[idKey];
            const sessionId = Number(row?.session_id);
            if (!Number.isFinite(sessionId)) {
                return;
            }

            const identifier = (rawId === null || rawId === undefined || rawId === '')
                ? (idKey === 'therapist_id' ? 'unknown_therapist' : 'Unknown')
                : rawId;

            const sessionIndex = sessionIndexMap.get(sessionId);
            if (sessionIndex === undefined) {
                return;
            }

            if (!dataByIdentifier.has(identifier)) {
                const resolvedLabel = labelResolver ? labelResolver(identifier) : identifier;
                dataByIdentifier.set(identifier, {
                    id: identifier,
                    label: resolvedLabel || 'Unknown',
                    data: new Array(totalSessions).fill(null)
                });
            }

            const entry = dataByIdentifier.get(identifier);
            const rawValue = valueKey ? row?.[valueKey] : row?.avg_sure;
            entry.data[sessionIndex] = toNumericOrNull(rawValue);
        });

        const datasets = [];
        let index = 0;
        dataByIdentifier.forEach(entry => {
            datasets.push({
                id: entry.id,
                label: entry.label,
                data: entry.data.slice(),
                index
            });
            index += 1;
        });

        return {
            sessions: sessionNumbers,
            labels: sessionLabels,
            datasets
        };
    }


    function updateInSessionWarningSignsChart(baseParams) {
        if (!selectedWarningConstruct) {
            return;
        }

        beginLoading();
        const params = baseParams instanceof URLSearchParams
            ? new URLSearchParams(baseParams)
            : new URLSearchParams(baseParams || '');
        params.set('construct', selectedWarningConstruct);

        fetch(`/api/in-session-warning-signs?${params.toString()}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load warning sign data');
                }
                return res.json();
            })
            .then(apiData => {
                const sessionDetailsMap = {};
                (apiData || []).forEach(entry => {
                    if (entry.construct_value === null || entry.construct_value === undefined) {
                        return;
                    }

                    const sessionId = String(entry.session_id);
                    const numericValue = Number(entry.construct_value);
                    const turnNumber = entry.turn !== undefined ? Number(entry.turn) : null;
                    const patientMessage = typeof entry.patient_message === 'string' ? entry.patient_message : '';
                    const previousTherapistMessage = typeof entry.previous_therapist_message === 'string' ? entry.previous_therapist_message : '';
                    const previousTherapistTurn = entry.previous_therapist_turn !== undefined ? Number(entry.previous_therapist_turn) : null;
                    const chainOfThought = Array.isArray(entry.chain_of_thought) ? entry.chain_of_thought : [];

                    if (!Number.isFinite(numericValue)) {
                        return;
                    }

                    if (!sessionDetailsMap[sessionId]) {
                        sessionDetailsMap[sessionId] = [];
                    }

                    sessionDetailsMap[sessionId].push({
                        turn: Number.isFinite(turnNumber) ? turnNumber : null,
                        value: numericValue,
                        patientMessage,
                        previousTherapistMessage,
                        previousTherapistTurn: Number.isFinite(previousTherapistTurn) ? previousTherapistTurn : null,
                        chainOfThought
                    });
                });

                const sessionKeys = Object.keys(sessionDetailsMap).sort((a, b) => Number(a) - Number(b));
                const datasetValues = sessionKeys.map(session => {
                    const details = sessionDetailsMap[session];
                    details.sort((a, b) => {
                        const aTurn = Number.isFinite(a.turn) ? a.turn : Number.POSITIVE_INFINITY;
                        const bTurn = Number.isFinite(b.turn) ? b.turn : Number.POSITIVE_INFINITY;
                        return aTurn - bTurn;
                    });
                    return details.map(item => item.value);
                });
                const labels = sessionKeys.map(session => `Session ${session}`);

                const chartLabel = `${getWarningConstructLabel(selectedWarningConstruct)} (patient turns)`;

                warningSignsSessionDetails = {};
                warningSignsSessionOrder = sessionKeys.slice();
                sessionKeys.forEach(session => {
                    warningSignsSessionDetails[session] = sessionDetailsMap[session] || [];
                });

                if (warningSignsDetailsContainer) {
                    warningSignsDetailsContainer.innerHTML = '';
                }

                warningSignsAggregatedState = {
                    sessionKeys: sessionKeys.slice(),
                    labels: labels.slice(),
                    datasetValues: datasetValues.map(values => values.slice()),
                    chartLabel
                };

                renderWarningSignsAggregatedChart();
            })
            .catch(error => {
                console.error('Error fetching warning sign data:', error);
                if (warningSignsFootnote) {
                    warningSignsFootnote.textContent = 'Unable to load data.';
                }
            })
            .finally(() => endLoading());
    }

    function attachWarningSignsClickHandler() {
        const chart = charts.warningSignsChart;
        if (!chart) return;

        const canvas = chart.canvas;
        if (!canvas) return;

        if (!warningSignsClickHandlerBound) {
            canvas.addEventListener('click', handleWarningSignsClick);
            warningSignsClickHandlerBound = true;
        }
    }

    function handleWarningSignsClick(event) {
        const chart = charts.warningSignsChart;
        if (!chart) return;

        if (warningSignsActiveSession) {
            handleWarningSignsSessionPointClick(chart, event);
            return;
        }

        if (!selectedPairingId) {
            window.alert('Select a pairing ID override to inspect individual session values.');
            return;
        }

        let dataIndex = null;
        const elements = chart.getElementsAtEventForMode(event, 'nearest', { intersect: true }, false);
        if (elements && elements.length > 0) {
            dataIndex = elements[0].index;
        }

        if (dataIndex === null || dataIndex === undefined) {
            const axisElements = chart.getElementsAtEventForMode(event, 'nearest', { intersect: false, axis: 'x' }, false);
            if (axisElements && axisElements.length > 0) {
                dataIndex = axisElements[0].index;
            }
        }

        const scales = chart.scales || {};
        const xScale = scales.x || Object.values(scales)[0];

        if ((dataIndex === null || dataIndex === undefined) && xScale && typeof xScale.getValueForPixel === 'function') {
            const nativeEvent = event.native || event;
            if (nativeEvent && nativeEvent.clientX !== undefined) {
                const rect = chart.canvas.getBoundingClientRect();
                const pixelX = nativeEvent.clientX - rect.left;
                const tentativeIndex = xScale.getValueForPixel(pixelX);
                if (Number.isFinite(tentativeIndex)) {
                    dataIndex = Math.round(tentativeIndex);
                }
            }
        }

        if (dataIndex === null || dataIndex === undefined) {
            return;
        }

        if (dataIndex < 0 || dataIndex >= warningSignsSessionOrder.length) {
            return;
        }

        const sessionId = warningSignsSessionOrder[dataIndex];
        if (!sessionId) {
            return;
        }

        renderWarningSignsSessionChart(sessionId);
    }

    function renderWarningSignsSessionChart(sessionId) {
        const details = (warningSignsSessionDetails[sessionId] || []).slice();
        if (!details.length) {
            window.alert(`No construct values available for Session ${sessionId}.`);
            return;
        }

        const sortedDetails = details.sort((a, b) => {
            const aTurn = Number.isFinite(a.turn) ? a.turn : Number.POSITIVE_INFINITY;
            const bTurn = Number.isFinite(b.turn) ? b.turn : Number.POSITIVE_INFINITY;
            return aTurn - bTurn;
        });

        const labels = sortedDetails.map((detail, index) => {
            if (Number.isFinite(detail.turn)) {
                return `Turn ${detail.turn}`;
            }
            return `Entry ${index + 1}`;
        });
        const values = sortedDetails.map(detail => detail.value);

        createOrUpdateChart('warningSignsChart', 'line', {
            labels,
            datasets: [{
                label: `${getWarningConstructLabel(selectedWarningConstruct)} — Session ${sessionId}`,
                data: values,
                borderColor: 'rgba(220, 53, 69, 0.85)',
                backgroundColor: 'rgba(220, 53, 69, 0.35)',
                borderWidth: 2,
                pointRadius: 4,
                pointHoverRadius: 6,
                tension: 0.2,
                fill: false
            }]
        }, {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    title: { display: true, text: 'Patient Turns' },
                    ticks: { autoSkip: false }
                },
                y: {
                    min: 1,
                    max: 5,
                    ticks: { stepSize: 1 },
                    title: { display: true, text: 'Intensity Score (1-5)' }
                }
            },
            plugins: {
                legend: { display: false }
            }
        });

        warningSignsActiveSession = sessionId;
        warningSignsActiveSessionDetails = sortedDetails;

        if (warningSignsDetailsContainer) {
            warningSignsDetailsContainer.innerHTML = `
                <div class="d-flex flex-column flex-md-row align-items-md-center justify-content-between gap-2 mt-3">
                    <div class="text-muted small">Showing patient turn values for Session ${sessionId}. Click a point to view conversation context.</div>
                    <div>
                        <button id="warningSignsBackToAggregated" class="btn btn-outline-secondary btn-sm">Back to Sessions</button>
                    </div>
                </div>
            `;

            const resetBtn = document.getElementById('warningSignsBackToAggregated');
            if (resetBtn) {
                resetBtn.addEventListener('click', () => {
                    renderWarningSignsAggregatedChart();
                });
            }
        }

        if (warningSignsFootnote) {
            warningSignsFootnote.textContent = `Patient turn values for Session ${sessionId}. Click any point to view context or "Back to Sessions" to return to boxplots.`;
        }
    }

    function renderWarningSignsAggregatedChart() {
        if (!warningSignsAggregatedState) {
            return;
        }

        const { labels, datasetValues, chartLabel, sessionKeys } = warningSignsAggregatedState;

        const medianAnnotations = {};
        labels.forEach((label, index) => {
            const median = calculateMedian(datasetValues[index]);
            if (!Number.isFinite(median)) {
                return;
            }
            const annotationKey = `median-${sessionKeys[index] ?? index}`;
            medianAnnotations[annotationKey] = {
                type: 'label',
                xScaleID: 'x',
                yScaleID: 'y',
                xValue: label,
                yValue: median,
                content: [`Median ${formatMedianLabel(median)}`],
                backgroundColor: 'rgba(220, 53, 69, 0.9)',
                color: '#ffffff',
                font: { size: 11, weight: '600' },
                padding: { top: 4, right: 6, bottom: 4, left: 6 },
                borderRadius: 4,
                yAdjust: -8
            };
        });

        const statsData = datasetValues.map(values => {
            const summary = calculateQuartileSummary(values);
            if (!summary) {
                return null;
            }
            const { min, q1, median, q3, max, values: numeric } = summary;
            return {
                min,
                q1,
                median,
                q3,
                max,
                items: numeric
            };
        });

        createOrUpdateChart('warningSignsChart', 'boxplot', {
            labels: labels.slice(),
            datasets: [{
                label: chartLabel,
                data: statsData,
                backgroundColor: 'rgba(13, 110, 253, 0.25)',
                borderColor: 'rgba(13, 110, 253, 0.8)',
                borderWidth: 1,
                outlierColor: 'rgba(220, 53, 69, 0.85)',
                outlierRadius: 4,
                medianColor: 'rgba(220, 53, 69, 0.95)',
                medianWidth: 2
            }]
        }, {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    min: 1,
                    max: 5,
                    ticks: { stepSize: 1 },
                    title: { display: true, text: 'Intensity Score (1-5)' }
                }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        title: context => context[0]?.label || '',
                        label: context => {
                            const stats = context.raw;
                            if (!stats || typeof stats !== 'object') {
                                return '';
                            }
                            const pieces = [];
                            if (stats.min !== undefined) pieces.push(`Min: ${formatMedianLabel(stats.min)}`);
                            if (stats.q1 !== undefined) pieces.push(`Q1 (25th): ${formatMedianLabel(stats.q1)}`);
                            if (stats.median !== undefined) pieces.push(`Median: ${formatMedianLabel(stats.median)}`);
                            if (stats.q3 !== undefined) pieces.push(`Q3 (75th): ${formatMedianLabel(stats.q3)}`);
                            if (stats.max !== undefined) pieces.push(`Max: ${formatMedianLabel(stats.max)}`);
                            return pieces.join(' | ');
                        }
                    }
                },
                annotation: {
                    clip: false,
                    annotations: medianAnnotations
                }
            },
            elements: {
                boxAndWhiskers: {
                    medianColor: 'rgba(220, 53, 69, 0.95)',
                    medianWidth: 2
                }
            }
        });

        warningSignsActiveSession = null;
        warningSignsActiveSessionDetails = [];

        if (warningSignsDetailsContainer) {
            warningSignsDetailsContainer.innerHTML = '';
        }

        if (warningSignsFootnote) {
            if (!sessionKeys.length) {
                warningSignsFootnote.textContent = 'No data available for the selected filters.';
            } else if (selectedPairingId) {
                warningSignsFootnote.textContent = `${warningSignsDefaultFootnote} Click a box to inspect patient turns.`;
            } else {
                warningSignsFootnote.textContent = `${warningSignsDefaultFootnote} Input a pairing id override to drill into patient turns.`;
            }
        }

        attachWarningSignsClickHandler();
    }

    function handleWarningSignsSessionPointClick(chart, event) {
        const elements = chart.getElementsAtEventForMode(event, 'nearest', { intersect: true }, false);
        if (!elements || elements.length === 0) {
            return;
        }

        const dataIndex = elements[0].index;
        if (dataIndex === null || dataIndex === undefined) {
            return;
        }

        if (dataIndex < 0 || dataIndex >= warningSignsActiveSessionDetails.length) {
            return;
        }

        const detail = warningSignsActiveSessionDetails[dataIndex];
        if (!detail) {
            return;
        }

        showWarningSignsTurnModal(warningSignsActiveSession, detail);
    }

    function showWarningSignsTurnModal(sessionId, detail) {
        const modalHandle = ensureTranscriptModal();
        if (!modalHandle) {
            return;
        }

        transcriptModalLastTrigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;

        const modalTitle = document.getElementById('transcriptModalTitle');
        const modalBody = document.getElementById('transcriptModalBody');

        const patientTurnLabel = Number.isFinite(detail.turn) ? detail.turn : '—';
        const therapistTurnLabel = Number.isFinite(detail.previousTherapistTurn) ? detail.previousTherapistTurn : '—';
        const therapistMessage = detail.previousTherapistMessage ? formatMultiline(detail.previousTherapistMessage) : '<span class="text-muted">No therapist response captured for the previous turn.</span>';
        const patientMessage = detail.patientMessage ? formatMultiline(detail.patientMessage) : '<span class="text-muted">No patient response recorded for this turn.</span>';

        let chainHtml = '<li class="text-muted">No chain of thought captured for this turn.</li>';
        if (Array.isArray(detail.chainOfThought) && detail.chainOfThought.length > 0) {
            const filteredChain = detail.chainOfThought.filter(item => {
                const label = (item.label || item.id || '').trim();
                return !WARNING_SIGNS_CHAIN_LABELS_TO_HIDE.has(label);
            });

            if (filteredChain.length > 0) {
                chainHtml = filteredChain.map(item => {
                    const label = escapeHtml(item.label || item.id || 'Detail');
                    const value = escapeHtml(item.value);
                    return `<li><strong>${label}:</strong> ${value}</li>`;
                }).join('');
            } else {
                chainHtml = '<li class="text-muted">Psychological construct scores are omitted in this view.</li>';
            }
        }

        if (modalTitle) {
            modalTitle.textContent = `Session ${sessionId} — Patient Turn ${patientTurnLabel}`;
        }

        if (modalBody) {
            modalBody.innerHTML = `
                <div class="mb-3">
                    <h6 class="text-uppercase text-muted mb-2">Previous Therapist Response</h6>
                    <p class="mb-1"><strong>Turn ${therapistTurnLabel}</strong></p>
                    <p class="mb-0">${therapistMessage}</p>
                </div>
                <div class="mb-3">
                    <h6 class="text-uppercase text-muted mb-2">Patient Chain of Thought</h6>
                    <ul class="list-unstyled mb-0">${chainHtml}</ul>
                </div>
                <div>
                    <h6 class="text-uppercase text-muted mb-2">Patient Response</h6>
                    <p class="mb-0">${patientMessage}</p>
                </div>
            `;
        }

    modalHandle.modal.show();
    }

    function updateMiGlobalProfileChart(query) {
        beginLoading();
        fetch(`/api/mi-global-profile?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load MI global profile');
                }
                return res.json();
            }).then(apiData => {
                const labels = ['Cultivating Change Talk', 'Softening Sustain Talk', 'Partnership', 'Empathy'];

                if (!apiData || apiData.length === 0) {
                    createOrUpdateChart('miGlobalProfileChart', 'radar', {
                        labels,
                        datasets: []
                    }, {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: { r: { beginAtZero: true, max: 5 } },
                        plugins: { legend: { position: 'bottom' } }
                    });
                    return;
                }

                const palette = [
                    { border: 'rgba(54, 162, 235, 1)', background: 'rgba(54, 162, 235, 0.2)' },
                    { border: 'rgba(255, 99, 132, 1)', background: 'rgba(255, 99, 132, 0.2)' },
                    { border: 'rgba(255, 205, 86, 1)', background: 'rgba(255, 205, 86, 0.2)' },
                    { border: 'rgba(75, 192, 192, 1)', background: 'rgba(75, 192, 192, 0.2)' },
                    { border: 'rgba(153, 102, 255, 1)', background: 'rgba(153, 102, 255, 0.2)' },
                    { border: 'rgba(255, 159, 64, 1)', background: 'rgba(255, 159, 64, 0.2)' }
                ];

                const datasets = apiData.map((row, index) => {
                    const color = palette[index % palette.length];
                    const baseLabel = row.therapist_id
                        ? getTherapistDisplayName(row.therapist_id)
                        : `Therapist ${index + 1}`;
                    const sessionCount = Number(row.session_count) || 0;
                    const therapistLabel = formatSessionsLabel(baseLabel, sessionCount);
                    return {
                        label: therapistLabel,
                        data: [
                            Number(row.cultivating_change_talk) || 0,
                            Number(row.softening_sustain_talk) || 0,
                            Number(row.partnership) || 0,
                            Number(row.empathy) || 0
                        ],
                        fill: true,
                        backgroundColor: color.background,
                        borderColor: color.border,
                        pointBackgroundColor: color.border,
                        pointHoverBackgroundColor: color.border,
                        pointBorderColor: '#ffffff',
                        borderWidth: 2
                    };
                });

                createOrUpdateChart('miGlobalProfileChart', 'radar', {
                    labels,
                    datasets
                }, {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        r: {
                            beginAtZero: true,
                            max: 5,
                            ticks: { stepSize: 1 }
                        }
                    },
                    plugins: {
                        legend: {
                            position: 'bottom'
                        }
                    }
                });
            })
            .catch(error => console.error('Error fetching MI global profile:', error))
            .finally(() => endLoading());
    }

    function updateMiGlobalMetrics(query) {
        beginLoading();
        fetch(`/api/mi-global-metrics?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load MI global metrics');
                }
                return res.json();
            }).then(apiData => {
                const rows = Array.isArray(apiData) ? apiData : [];

                const labels = [];
                const backgroundColors = [];
                const borderColors = [];
                const technicalValues = [];
                const relationalValues = [];

                rows.forEach(row => {
                    const therapistId = row.therapist_id || `therapist_${labels.length + 1}`;
                    const baseLabel = getTherapistDisplayName(therapistId);
                    const sessionCount = Number(row.session_count) || 0;
                    labels.push(formatSessionsLabel(baseLabel, sessionCount));
                    const colors = getTherapistColors(therapistId);
                    backgroundColors.push(colors.background);
                    borderColors.push(colors.border);
                    technicalValues.push(Number(row.technical_global) || 0);
                    relationalValues.push(Number(row.relational_global) || 0);
                });

                const buildXAxis = () => ({
                    ticks: { autoSkip: false, maxRotation: 45, minRotation: 0 }
                });

                createOrUpdateChart('technicalGlobalChart', 'bar', {
                    labels,
                    datasets: [{
                        label: 'Technical Global Score',
                        data: technicalValues,
                        backgroundColor: technicalValues.length ? backgroundColors.slice() : [],
                        borderColor: technicalValues.length ? borderColors.slice() : [],
                        borderWidth: 1
                    }]
                }, {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: buildXAxis(),
                        y: {
                            beginAtZero: true,
                            max: 5,
                            title: { display: true, text: 'Score' }
                        }
                    },
                    plugins: {
                        legend: { display: false },
                        annotation: {
                            annotations: {
                                beginningLine: {
                                    type: 'line',
                                    yMin: 3,
                                    yMax: 3,
                                    borderColor: THRESHOLD_COLORS.beginning,
                                    borderWidth: 2,
                                    borderDash: [5, 5],
                                    label: buildThresholdLabel('Beginning Proficiency (3.0)', THRESHOLD_COLORS.beginning, '#212529')
                                },
                                competencyLine: {
                                    type: 'line',
                                    yMin: 4,
                                    yMax: 4,
                                    borderColor: THRESHOLD_COLORS.competency,
                                    borderWidth: 2,
                                    borderDash: [5, 5],
                                    label: buildThresholdLabel('Competency (4.0)', THRESHOLD_COLORS.competency)
                                }
                            }
                        }
                    }
                });

                createOrUpdateChart('relationalGlobalChart', 'bar', {
                    labels,
                    datasets: [{
                        label: 'Relational Global Score',
                        data: relationalValues,
                        backgroundColor: relationalValues.length ? backgroundColors.slice() : [],
                        borderColor: relationalValues.length ? borderColors.slice() : [],
                        borderWidth: 1
                    }]
                }, {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: buildXAxis(),
                        y: {
                            beginAtZero: true,
                            max: 5,
                            title: { display: true, text: 'Score' }
                        }
                    },
                    plugins: {
                        legend: { display: false },
                        annotation: {
                            annotations: {
                                beginningLine: {
                                    type: 'line',
                                    yMin: 3.5,
                                    yMax: 3.5,
                                    borderColor: THRESHOLD_COLORS.beginning,
                                    borderWidth: 2,
                                    borderDash: [5, 5],
                                    label: buildThresholdLabel('Beginning Proficiency (3.5)', THRESHOLD_COLORS.beginning, '#212529')
                                },
                                competencyLine: {
                                    type: 'line',
                                    yMin: 4,
                                    yMax: 4,
                                    borderColor: THRESHOLD_COLORS.competency,
                                    borderWidth: 2,
                                    borderDash: [5, 5],
                                    label: buildThresholdLabel('Competency (4.0)', THRESHOLD_COLORS.competency)
                                }
                            }
                        }
                    }
                });
            })
            .catch(error => console.error('Error fetching MI global metrics:', error))
            .finally(() => endLoading());
    }

    function updateMiBehaviorMetrics(query) {
        beginLoading();
        fetch(`/api/mi-behavior-metrics?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load MI behavior metrics');
                }
                return res.json();
            })
            .then(apiData => {
                const rows = Array.isArray(apiData) ? apiData : [];

                const labels = [];
                const backgroundColors = [];
                const borderColors = [];
                const percentCrValues = [];
                const rqRatioValues = [];
                const percentMiAdherentValues = [];

                rows.forEach(row => {
                    const therapistId = row.therapist_id || `therapist_${labels.length + 1}`;
                    const baseLabel = getTherapistDisplayName(therapistId);
                    const sessionCount = Number(row.session_count) || 0;
                    labels.push(formatSessionsLabel(baseLabel, sessionCount));
                    const colors = getTherapistColors(therapistId);
                    backgroundColors.push(colors.background);
                    borderColors.push(colors.border);
                    percentCrValues.push(Number(row.percent_cr) || 0);
                    rqRatioValues.push(Number(row.r_q_ratio) || 0);
                    percentMiAdherentValues.push(Number(row.percent_mi_adherent) || 0);
                });

                const buildXAxis = () => ({
                    ticks: { autoSkip: false, maxRotation: 45, minRotation: 0 }
                });

                createOrUpdateChart('percentCrChart', 'bar', {
                    labels,
                    datasets: [{
                        label: '% Complex Reflections',
                        data: percentCrValues,
                        backgroundColor: percentCrValues.length ? backgroundColors.slice() : [],
                        borderColor: percentCrValues.length ? borderColors.slice() : [],
                        borderWidth: 1
                    }]
                }, {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: buildXAxis(),
                        y: {
                            beginAtZero: true,
                            max: 100,
                            title: { display: true, text: 'Percentage (%)' }
                        }
                    },
                    plugins: {
                        legend: { display: false },
                        annotation: {
                            annotations: {
                                beginningLine: {
                                    type: 'line',
                                    yMin: 40,
                                    yMax: 40,
                                    borderColor: THRESHOLD_COLORS.beginning,
                                    borderWidth: 2,
                                    borderDash: [5, 5],
                                    label: buildThresholdLabel('Beginning Proficiency (40%)', THRESHOLD_COLORS.beginning, '#212529')
                                },
                                competencyLine: {
                                    type: 'line',
                                    yMin: 50,
                                    yMax: 50,
                                    borderColor: THRESHOLD_COLORS.competency,
                                    borderWidth: 2,
                                    borderDash: [5, 5],
                                    label: buildThresholdLabel('Competency (50%)', THRESHOLD_COLORS.competency)
                                }
                            }
                        }
                    }
                });

                createOrUpdateChart('rqRatioChart', 'bar', {
                    labels,
                    datasets: [{
                        label: 'Reflection to Question Ratio',
                        data: rqRatioValues,
                        backgroundColor: rqRatioValues.length ? backgroundColors.slice() : [],
                        borderColor: rqRatioValues.length ? borderColors.slice() : [],
                        borderWidth: 1
                    }]
                }, {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: buildXAxis(),
                        y: {
                            beginAtZero: true,
                            max: 3,
                            title: { display: true, text: 'Ratio' }
                        }
                    },
                    plugins: {
                        legend: { display: false },
                        annotation: {
                            annotations: {
                                beginningLine: {
                                    type: 'line',
                                    yMin: 1,
                                    yMax: 1,
                                    borderColor: THRESHOLD_COLORS.beginning,
                                    borderWidth: 2,
                                    borderDash: [5, 5],
                                    label: buildThresholdLabel('Beginning Proficiency (1:1)', THRESHOLD_COLORS.beginning, '#212529')
                                },
                                competencyLine: {
                                    type: 'line',
                                    yMin: 2,
                                    yMax: 2,
                                    borderColor: THRESHOLD_COLORS.competency,
                                    borderWidth: 2,
                                    borderDash: [5, 5],
                                    label: buildThresholdLabel('Competency (2:1)', THRESHOLD_COLORS.competency)
                                }
                            }
                        }
                    }
                });

                createOrUpdateChart('percentMiaChart', 'bar', {
                    labels,
                    datasets: [{
                        label: '% MI-Adherent',
                        data: percentMiAdherentValues,
                        backgroundColor: percentMiAdherentValues.length ? backgroundColors.slice() : [],
                        borderColor: percentMiAdherentValues.length ? borderColors.slice() : [],
                        borderWidth: 1
                    }]
                }, {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: buildXAxis(),
                        y: {
                            beginAtZero: true,
                            max: 100,
                            title: { display: true, text: 'Percentage (%)' }
                        }
                    },
                    plugins: {
                        legend: { display: false },
                        annotation: {
                            annotations: {
                                beginningLine: {
                                    type: 'line',
                                    yMin: 90,
                                    yMax: 90,
                                    borderColor: THRESHOLD_COLORS.beginning,
                                    borderWidth: 2,
                                    borderDash: [5, 5],
                                    label: buildThresholdLabel('Beginning Proficiency (90%)', THRESHOLD_COLORS.beginning, '#212529')
                                },
                                competencyLine: {
                                    type: 'line',
                                    yMin: 100,
                                    yMax: 100,
                                    borderColor: THRESHOLD_COLORS.competency,
                                    borderWidth: 2,
                                    borderDash: [5, 5],
                                    label: buildThresholdLabel('Competency (100%)', THRESHOLD_COLORS.competency)
                                }
                            }
                        }
                    }
                });

                const percentCrEl = document.getElementById('miPercentCR');
                if (percentCrEl) {
                    const avgCr = percentCrValues.length ? percentCrValues.reduce((sum, value) => sum + value, 0) / percentCrValues.length : 0;
                    percentCrEl.textContent = percentCrValues.length ? `${avgCr.toFixed(1)}%` : '—';
                }
                const rqRatioEl = document.getElementById('miRQRatio');
                if (rqRatioEl) {
                    const avgRq = rqRatioValues.length ? rqRatioValues.reduce((sum, value) => sum + value, 0) / rqRatioValues.length : 0;
                    rqRatioEl.textContent = rqRatioValues.length ? avgRq.toFixed(2) : '—';
                }
                const percentAdherentEl = document.getElementById('miPercentAdherent');
                if (percentAdherentEl) {
                    const avgMia = percentMiAdherentValues.length ? percentMiAdherentValues.reduce((sum, value) => sum + value, 0) / percentMiAdherentValues.length : 0;
                    percentAdherentEl.textContent = percentMiAdherentValues.length ? `${avgMia.toFixed(1)}%` : '—';
                }
            })
            .catch(error => console.error('Error fetching MI behavior metrics:', error))
            .finally(() => endLoading());
    }

    function updateSrsComponentTrends(query) {
        beginLoading();
        fetch(`/api/srs-session-component-trends?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load SRS component trends');
                }
                return res.json();
            })
            .then(apiData => {
                const therapistRecords = apiData && Array.isArray(apiData.therapist) ? apiData.therapist : [];
                const patientRecords = apiData && Array.isArray(apiData.patient) ? apiData.patient : [];

                srsTrendCache[SRS_VIEW_THERAPIST] = transformSrsTrendRecords(
                    therapistRecords,
                    'therapist_id',
                    id => {
                        if (!id || id === 'unknown_therapist') {
                            return 'Unknown Therapist';
                        }
                        const displayName = getTherapistDisplayName(id);
                        return displayName || 'Unknown Therapist';
                    }
                );

                srsTrendCache[SRS_VIEW_PATIENT] = transformSrsTrendRecords(
                    patientRecords,
                    'subtype_name',
                    id => (id && id !== 'Unknown') ? id : 'Unknown Subtype'
                );

                if (currentSrsOutcomeLevel === SRS_OUTCOME_LEVEL_SESSION) {
                    renderSrsOutcomeChart();
                }
            })
            .catch(error => console.error('Error fetching SRS component trends:', error))
            .finally(() => endLoading());
    }

    function updateWaiComponentTrends(query) {
        beginLoading();
        fetch(`/api/wai-session-component-trends?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load WAI component trends');
                }
                return res.json();
            })
            .then(apiData => {
                const therapistRecords = apiData && Array.isArray(apiData.therapist) ? apiData.therapist : [];
                const patientRecords = apiData && Array.isArray(apiData.patient) ? apiData.patient : [];

                waiTrendCache[WAI_ENTITY_THERAPIST] = transformWaiTrendRecords(
                    therapistRecords,
                    'therapist_id',
                    id => {
                        if (!id || id === 'unknown_therapist') {
                            return 'Unknown Therapist';
                        }
                        const displayName = getTherapistDisplayName(id);
                        return displayName || 'Unknown Therapist';
                    }
                );

                waiTrendCache[WAI_ENTITY_PATIENT] = transformWaiTrendRecords(
                    patientRecords,
                    'subtype_name',
                    id => (id && id !== 'Unknown') ? id : 'Unknown Subtype'
                );

                renderWaiUnifiedChart();
            })
            .catch(error => console.error('Error fetching WAI component trends:', error))
            .finally(() => endLoading());
    }

    function updateAdverseOutcomesChart(query) {
        beginLoading();
        const summaryEl = document.getElementById('adverseOutcomeSummary');
        if (summaryEl) {
            const sessionsSelected = selectedFilters.session;
            const sortedSessions = sessionsSelected.map(Number).sort((a, b) => a - b);
            let sessionLabel;
            if (sortedSessions.length === 0) {
                sessionLabel = 'All sessions shown';
            } else if (sortedSessions.length === 1) {
                sessionLabel = `Session ${sortedSessions[0]} shown`;
            } else {
                sessionLabel = `Sessions ${sortedSessions.join(', ')} shown`;
            }
            summaryEl.textContent = `${sessionLabel} (0 sessions)`;
        }

        fetch(`/api/adverse-outcomes?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load adverse outcomes');
                }
                return res.json();
            })
            .then(apiData => {
                if (apiData.length === 0) {
                    if (summaryEl) {
                        summaryEl.textContent += ' - no data available';
                    }
                    return;
                }

                const data = apiData[0];
                const totalSessions = data.total_sessions || 0;

                if (summaryEl) {
                    const sessionsSelected = selectedFilters.session;
                    const sortedSessions = sessionsSelected.map(Number).sort((a, b) => a - b);
                    let sessionLabel;
                    if (sortedSessions.length === 0) {
                        sessionLabel = 'All sessions shown';
                    } else if (sortedSessions.length === 1) {
                        sessionLabel = `Session ${sortedSessions[0]} shown`;
                    } else {
                        sessionLabel = `Sessions ${sortedSessions.join(', ')} shown`;
                    }
                    const sessionCountLabel = `${totalSessions} session${totalSessions === 1 ? '' : 's'}`;
                    summaryEl.textContent = `${sessionLabel} (${sessionCountLabel})`;
                }

                // Create readable labels and extract counts
                const outcomeLabels = {
                    'death_by_suicide': 'Death by Suicide',
                    'suicide_attempt': 'Suicide Attempt',
                    'non_suicidal_self_injury': 'Non-Suicidal Self-Injury',
                    'relapse_substance_use': 'Relapse (Substance Use)',
                    'increase_alcohol_seeking': 'Increase Alcohol Seeking',
                    'neglect_of_roles': 'Neglect of Roles',
                    'treatment_dropout': 'Treatment Dropout',
                    'intensification_suicidal_ideation': 'Intensification Suicidal Ideation',
                    'intensification_shame_stigma': 'Intensification Shame/Stigma',
                    'interpersonal_functioning_decline': 'Interpersonal Functioning Decline',
                    'no_adverse_outcome': 'No Adverse Outcome'
                };

                const labels = [];
                const percentageValues = [];
                const rawCounts = [];

                Object.keys(outcomeLabels).forEach(key => {
                    if (data[key] !== undefined) {
                        const count = data[key];
                        const percentage = totalSessions ? (count / totalSessions) * 100 : 0;
                        labels.push(outcomeLabels[key]);
                        percentageValues.push(Number(percentage.toFixed(2)));
                        rawCounts.push(count);
                    }
                });

                createOrUpdateChart('adverseOutcomesChart', 'bar', {
                    labels: labels,
                    datasets: [{
                        label: 'Percent of Sessions',
                        data: percentageValues,
                        backgroundColor: 'rgba(220, 53, 69, 0.7)',
                        borderColor: 'rgba(220, 53, 69, 1)',
                        borderWidth: 1
                    }]
                }, {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    layout: {
                        padding: {
                            bottom: 28
                        }
                    },
                    scales: {
                        x: {
                            beginAtZero: true,
                            suggestedMax: 100,
                            title: { display: true, text: 'Percentage of Sessions (%)' },
                            ticks: {
                                callback: value => `${value}%`
                            }
                        }
                    },
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: context => {
                                    const pct = context.parsed.x ?? 0;
                                    const count = rawCounts[context.dataIndex] ?? 0;
                                    return `${pct.toFixed(1)}% (${count} of ${totalSessions} sessions)`;
                                }
                            }
                        }
                    }
                });
            })
            .catch(error => console.error('Error fetching adverse outcomes:', error))
            .finally(() => endLoading());
    }

    function updateAdverseAttributionsChart(baseParams) {
        beginLoading();

        if (selectedAdverseEvents.length === 0) {
            createOrUpdateChart('adverseAttributionsChart', 'pie', {
                labels: [],
                datasets: [{ data: [] }]
            }, {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'right',
                        labels: { boxWidth: 15, font: { size: 11 } }
                    }
                }
            });
            endLoading();
            return;
        }

        const params = baseParams instanceof URLSearchParams
            ? new URLSearchParams(baseParams.toString())
            : new URLSearchParams(baseParams || '');

        params.delete('events');
        selectedAdverseEvents.forEach(eventValue => params.append('events', eventValue));

        fetch(`/api/adverse-outcome-attributions?${params.toString()}`)
            .then(res => {
                if (!res.ok) throw new Error('Failed to load adverse outcome attributions');
                return res.json();
            })
            .then(apiData => {
                if (!Array.isArray(apiData) || apiData.length === 0) {
                    createOrUpdateChart('adverseAttributionsChart', 'pie', {
                        labels: [],
                        datasets: [{ data: [] }]
                    }, {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: {
                                position: 'right',
                                labels: { boxWidth: 15, font: { size: 11 } }
                            }
                        }
                    });
                    endLoading();
                    return;
                }
                
                // FILTER OUT "No Adverse Outcome" entries
                const excludedAttributions = new Set([
                    'no adverse outcome',
                    'no adverse outcomes',
                    'no adverse events',
                    'no adverse event'
                ]);
                const filteredData = apiData.filter(d => {
                    if (!d.attribution) return false;
                    const normalized = d.attribution.trim().toLowerCase();
                    return !excludedAttributions.has(normalized);
                });
                
                if (filteredData.length === 0) {
                    createOrUpdateChart('adverseAttributionsChart', 'pie', {
                        labels: ['No attributable adverse events'],
                        datasets: [{ data: [1], backgroundColor: ['rgba(200, 200, 200, 0.3)'] }]
                    }, {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { display: false }
                        }
                    });
                    endLoading();
                    return;
                }
                
                const combinedMap = new Map();
                const normalizedTarget = new Set(['your own action', 'your own actions', 'my own action', 'my own actions']);

                filteredData.forEach(entry => {
                    const rawLabel = (entry.attribution || '').trim();
                    const normalized = rawLabel.toLowerCase();
                    const mergedLabel = normalizedTarget.has(normalized) ? 'Your Own Actions' : rawLabel;
                    const current = combinedMap.get(mergedLabel) || 0;
                    combinedMap.set(mergedLabel, current + (entry.count || 0));
                });

                const labels = Array.from(combinedMap.keys());
                const values = Array.from(combinedMap.values());
                
                createOrUpdateChart('adverseAttributionsChart', 'pie', {
                    labels,
                    datasets: [{
                        data: values,
                        backgroundColor: [
                            'rgba(220, 53, 69, 0.7)',
                            'rgba(255, 193, 7, 0.7)',
                            'rgba(13, 110, 253, 0.7)',
                            'rgba(25, 135, 84, 0.7)',
                            'rgba(108, 117, 125, 0.7)',
                            'rgba(255, 99, 132, 0.7)',
                            'rgba(54, 162, 235, 0.7)',
                            'rgba(255, 206, 86, 0.7)'
                        ]
                    }]
                }, {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'right',
                            labels: {
                                boxWidth: 15,
                                font: { size: 11 }
                            }
                        }
                    }
                });
            })
            .catch(error => console.error('Error fetching adverse attributions:', error))
            .finally(() => endLoading());
    }

    // --- UTILITY & EVENT HANDLERS ---
    function createOrUpdateChart(canvasId, type, data, options) {
        const chartColors = {
            backgroundColor: ['rgba(54, 162, 235, 0.7)', 'rgba(255, 99, 132, 0.7)', 'rgba(255, 206, 86, 0.7)', 'rgba(75, 192, 192, 0.7)', 'rgba(153, 102, 255, 0.7)'],
        };
        if (data.datasets) data.datasets.forEach(ds => {
            if (!ds.backgroundColor) ds.backgroundColor = chartColors.backgroundColor;
        });

        if (charts[canvasId]) {
            if (charts[canvasId].config.type !== type) {
                charts[canvasId].destroy();
                delete charts[canvasId];
            }
        }

        if (!charts[canvasId]) {
            const ctx = document.getElementById(canvasId)?.getContext('2d');
            if (ctx) {
                charts[canvasId] = new Chart(ctx, { type, data, options });
            }
            return;
        }

        charts[canvasId].data = data;
        charts[canvasId].options = options;
        charts[canvasId].update();
    }

    function showTranscript(pairingId, subtype, therapist, sessionId, turn) {
        const modalHandle = ensureTranscriptModal();
        if (!modalHandle) {
            return;
        }

        transcriptModalLastTrigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;

        const pairingLabel = pairingId !== null && pairingId !== undefined ? String(pairingId) : 'Unknown';
        const subtypeLabel = subtype && String(subtype).trim().length ? String(subtype) : 'Unknown';
        const therapistLabel = therapist && String(therapist).trim().length ? String(therapist) : 'Unknown Therapist';
        const sessionLabel = sessionId !== null && sessionId !== undefined ? String(sessionId) : 'Unknown';
        const turnLabel = turn !== null && turn !== undefined ? String(turn) : 'Unknown';

        const modalTitle = document.getElementById('transcriptModalTitle');
        const modalBody = document.getElementById('transcriptModalBody');

        if (modalTitle) {
            modalTitle.textContent = `Snippet for Pairing ID: ${pairingLabel} · Patient Type: ${subtypeLabel} · Therapist: ${therapistLabel} · Session: ${sessionLabel} · Dialogue Turn: ${turnLabel}`;
        }

        if (modalBody) {
            modalBody.innerHTML = '<p class="text-center">Loading transcript...</p>';
        }

        modalHandle.modal.show();

        const pairingParam = Number(pairingId);
        const sessionParam = Number(sessionId);
        const turnParam = Number(turn);

        if (!Number.isFinite(pairingParam) || !Number.isFinite(sessionParam) || !Number.isFinite(turnParam)) {
            if (modalBody) {
                modalBody.innerHTML = '<p class="text-muted">Transcript is unavailable for this event.</p>';
            }
            return;
        }

        const params = new URLSearchParams({
            pairing_id: pairingParam,
            session_id: sessionParam,
            turn: turnParam
        });

        fetch(`/api/transcript-snippet?${params.toString()}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load transcript snippet');
                }
                return res.json();
            })
            .then(data => {
                if (!Array.isArray(data) || data.length === 0) {
                    if (modalBody) {
                        modalBody.innerHTML = '<p class="text-muted">No transcript entries found for this turn.</p>';
                    }
                    return;
                }

                let html = '<ul class="list-unstyled">';
                data.forEach(line => {
                    const turnValue = Number.isFinite(Number(line.turn)) ? Number(line.turn) : line.turn;
                    const isTarget = Number(turnValue) === turnParam;
                    const speakerLabel = escapeHtml(line.speaker || 'Unknown');
                    const messageHtml = formatMultiline(line.message || '');
                    html += `<li class="transcript-line ${isTarget ? 'target-turn' : ''}"><strong>Turn ${escapeHtml(String(turnValue))} (${speakerLabel}):</strong><p>${messageHtml}</p></li>`;
                });
                html += '</ul>';

                if (modalBody) {
                    modalBody.innerHTML = html;
                }
            })
            .catch(error => {
                console.error('Error fetching transcript snippet:', error);
                if (modalBody) {
                    modalBody.innerHTML = '<p class="text-muted">Unable to load transcript for this event.</p>';
                }
            });
    }
    
    function setupEventListeners() {
        if (pairingFilterInput) {
            pairingFilterInput.addEventListener('input', () => {
                pendingPairingId = pairingFilterInput.value;
                updateApplyButtonState();
            });

            pairingFilterInput.addEventListener('keydown', event => {
                if (event.key === 'Enter') {
                    event.preventDefault();
                    if (applyBtn) {
                        applyBtn.click();
                    }
                }
            });
        }


        if (warningSignsConstructSelect) {
            warningSignsConstructSelect.addEventListener('change', () => {
                selectedWarningConstruct = warningSignsConstructSelect.value;
                const params = buildFilterParams();
                updateInSessionWarningSignsChart(params);
            });
        }
        
        if (applyBtn) {
            applyBtn.addEventListener('click', async () => {
                if (!filtersChanged()) {
                    return;
                }

                applyBtn.disabled = true;

                const pairingValue = normalizePairingValue(pendingPairingId);

                if (pairingValue) {
                    const previousPairing = selectedPairingId;
                    try {
                        await applyPairingContext(pairingValue);
                    } catch (error) {
                        console.error('Failed to apply pairing context:', error);
                        selectedPairingId = previousPairing;
                        pendingPairingId = pairingFilterInput ? pairingFilterInput.value : '';
                        updateApplyButtonState();
                        return;
                    }

                    updateApplyButtonState();
                    updateDashboard();
                    return;
                }

                selectedPairingId = '';
                pendingPairingId = pairingFilterInput ? pairingFilterInput.value : '';

                if (pairingFilterInput) {
                    pairingFilterInput.value = pendingPairingId.trim();
                }

                filterKeys.forEach(key => {
                    selectedFilters[key] = [...pendingFilters[key]];
                    updateFilterButtonLabel(key);
                });

                syncPendingWithSelected();

                updateApplyButtonState();
                updateDashboard();
            });
        }

        if (applyAdverseEventsBtn) {
            applyAdverseEventsBtn.addEventListener('click', () => {
                if (!adverseEventsChanged()) {
                    return;
                }
                selectedAdverseEvents = [...pendingAdverseEvents];
                updateAdverseEventsButtonLabel();
                updateAdverseEventsApplyButtonState();
                updateAdverseAttributionsChart(buildFilterParams());
            });
        }

        if (resetAdverseEventsBtn) {
            resetAdverseEventsBtn.addEventListener('click', () => {
                pendingAdverseEvents = adverseAttributionOptions.map(option => option.value);
                selectedAdverseEvents = [...pendingAdverseEvents];

                if (adverseEventsMenu) {
                    adverseEventsMenu.querySelectorAll('input[data-event-value]').forEach(box => {
                        box.checked = true;
                    });
                }

                if (adverseEventsSelectAllCheckbox) {
                    adverseEventsSelectAllCheckbox.checked = true;
                }

                updateAdverseEventsButtonLabel();
                updateAdverseEventsApplyButtonState();
                updateAdverseAttributionsChart(buildFilterParams());
            });
        }

        if (applyEquityEventsBtn) {
            applyEquityEventsBtn.addEventListener('click', () => {
                if (!equityEventsChanged()) {
                    return;
                }
                selectedEquityEvents = [...pendingEquityEvents];
                updateEquityEventsButtonLabel();
                updateEquityEventsApplyButtonState();
                updateEquityAudit(buildFilterParams());
            });
        }

        if (resetEquityEventsBtn) {
            resetEquityEventsBtn.addEventListener('click', () => {
                pendingEquityEvents = adverseAttributionOptions.map(option => option.value);
                selectedEquityEvents = [...pendingEquityEvents];

                if (equityEventsMenu) {
                    equityEventsMenu.querySelectorAll('input[data-equity-event-value]').forEach(box => {
                        box.checked = true;
                    });
                }

                if (equityEventsSelectAllCheckbox) {
                    equityEventsSelectAllCheckbox.checked = true;
                }

                updateEquityEventsButtonLabel();
                updateEquityEventsApplyButtonState();
                updateEquityAudit(buildFilterParams());
            });
        }

        if (sureOutcomeLevelToggleButtons.length) {
            sureOutcomeLevelToggleButtons.forEach(button => {
                button.addEventListener('click', () => {
                    const levelValue = button.dataset.sureOutcomeLevel;
                    if (!levelValue || (levelValue !== SURE_OUTCOME_LEVEL_INTERVENTION && levelValue !== SURE_OUTCOME_LEVEL_SESSION)) {
                        return;
                    }
                    if (levelValue === currentSureOutcomeLevel) {
                        return;
                    }
                    currentSureOutcomeLevel = levelValue;
                    renderSureOutcomeChart();
                });
            });
        }

        if (sureOutcomeViewToggleButtons.length) {
            sureOutcomeViewToggleButtons.forEach(button => {
                button.addEventListener('click', () => {
                    const viewValue = button.dataset.sureOutcomeView;
                    if (!viewValue || (viewValue !== SURE_VIEW_THERAPIST && viewValue !== SURE_VIEW_PATIENT)) {
                        return;
                    }
                    if (viewValue === currentSureOutcomeView) {
                        return;
                    }
                    currentSureOutcomeView = viewValue;
                    renderSureOutcomeChart();
                });
            });
        }

        if (sureOutcomeMetricToggleButtons.length) {
            sureOutcomeMetricToggleButtons.forEach(button => {
                button.addEventListener('click', () => {
                    const metricValue = button.dataset.sureOutcomeMetric;
                    if (!metricValue || !SURE_OUTCOME_METRICS.includes(metricValue)) {
                        return;
                    }
                    if (metricValue === currentSureOutcomeMetric) {
                        return;
                    }
                    currentSureOutcomeMetric = metricValue;
                    renderSureOutcomeChart();
                });
            });
        }

        if (srsOutcomeViewToggleButtons.length) {
            srsOutcomeViewToggleButtons.forEach(button => {
                button.addEventListener('click', () => {
                    const viewValue = button.dataset.srsOutcomeView;
                    if (!viewValue || (viewValue !== SRS_VIEW_THERAPIST && viewValue !== SRS_VIEW_PATIENT)) {
                        return;
                    }
                    if (viewValue === currentSrsOutcomeView) {
                        return;
                    }
                    currentSrsOutcomeView = viewValue;
                    renderSrsOutcomeChart();
                });
            });
        }

        if (srsOutcomeLevelToggleButtons.length) {
            srsOutcomeLevelToggleButtons.forEach(button => {
                button.addEventListener('click', () => {
                    const levelValue = button.dataset.srsOutcomeLevel;
                    if (!levelValue || (levelValue !== SRS_OUTCOME_LEVEL_INTERVENTION && levelValue !== SRS_OUTCOME_LEVEL_SESSION)) {
                        return;
                    }
                    if (levelValue === currentSrsOutcomeLevel) {
                        return;
                    }
                    currentSrsOutcomeLevel = levelValue;
                    renderSrsOutcomeChart();
                });
            });
        }

        if (srsOutcomeMetricToggleButtons.length) {
            srsOutcomeMetricToggleButtons.forEach(button => {
                button.addEventListener('click', () => {
                    const metricValue = button.dataset.srsOutcomeMetric;
                    if (!metricValue || !SRS_OUTCOME_METRICS.includes(metricValue)) {
                        return;
                    }
                    if (metricValue === currentSrsOutcomeMetric) {
                        return;
                    }
                    currentSrsOutcomeMetric = metricValue;
                    renderSrsOutcomeChart();
                });
            });
        }

        if (waiModeToggleButtons.length) {
            waiModeToggleButtons.forEach(button => {
                button.addEventListener('click', () => {
                    const modeValue = button.dataset.waiMode;
                    if (!modeValue || (modeValue !== WAI_MODE_INTERVENTION && modeValue !== WAI_MODE_SESSION)) {
                        return;
                    }
                    if (modeValue === currentWaiMode) {
                        return;
                    }
                    currentWaiMode = modeValue;
                    renderWaiUnifiedChart();
                });
            });
        }

        if (waiEntityToggleButtons.length) {
            waiEntityToggleButtons.forEach(button => {
                button.addEventListener('click', () => {
                    const entityValue = button.dataset.waiEntity;
                    if (!entityValue || (entityValue !== WAI_ENTITY_THERAPIST && entityValue !== WAI_ENTITY_PATIENT)) {
                        return;
                    }
                    if (entityValue === currentWaiEntity) {
                        return;
                    }
                    currentWaiEntity = entityValue;
                    renderWaiUnifiedChart();
                });
            });
        }

        if (waiMetricToggleButtons.length) {
            waiMetricToggleButtons.forEach(button => {
                button.addEventListener('click', () => {
                    const metricValue = button.dataset.waiMetric;
                    if (!metricValue || !WAI_METRICS.includes(metricValue)) {
                        return;
                    }
                    if (metricValue === currentWaiMetric) {
                        return;
                    }
                    currentWaiMetric = metricValue;
                    renderWaiUnifiedChart();
                });
            });
        }

        if (!resetBtn) {
            return;
        }

        resetBtn.addEventListener('click', () => {
            // Clear all selected filters
            filterKeys.forEach(key => {
                selectedFilters[key] = [];
                pendingFilters[key] = [];
            });

            selectedPairingId = '';
            pendingPairingId = '';
            if (pairingFilterInput) {
                pairingFilterInput.value = '';
            }
            
            // Uncheck all checkboxes
            document.querySelectorAll('.dropdown-menu input[type="checkbox"]').forEach(checkbox => {
                checkbox.checked = false;
            });
            
            // Reset button labels
            updateFilterButtonLabel('therapist');
            updateFilterButtonLabel('subtype');
            updateFilterButtonLabel('state');
            updateFilterButtonLabel('session');
            

            if (warningSignsOptions.length && warningSignsConstructSelect) {
                selectedWarningConstruct = warningSignsOptions[0].value;
                warningSignsConstructSelect.value = selectedWarningConstruct;
            }

            currentSureOutcomeView = SURE_VIEW_THERAPIST;
            currentSureOutcomeLevel = SURE_OUTCOME_LEVEL_INTERVENTION;
            currentSureOutcomeMetric = SURE_OUTCOME_METRIC_TOTAL;

            currentWaiMode = WAI_MODE_INTERVENTION;
            currentWaiEntity = WAI_ENTITY_THERAPIST;
            currentWaiMetric = WAI_METRIC_TOTAL;

            ['ids', 'labels', 'values', 'total_sure_drug_use', 'total_sure_self_care', 'total_sure_relationships', 'total_sure_material_resources', 'total_sure_outlook'].forEach(key => {
                if (sureCompositeCache[SURE_VIEW_THERAPIST]?.[key]) {
                    sureCompositeCache[SURE_VIEW_THERAPIST][key] = [];
                }
                if (sureCompositeCache[SURE_VIEW_PATIENT]?.[key]) {
                    sureCompositeCache[SURE_VIEW_PATIENT][key] = [];
                }
                if (sureDomainAggregateCache[SURE_VIEW_THERAPIST]?.[key] !== undefined) {
                    sureDomainAggregateCache[SURE_VIEW_THERAPIST][key] = [];
                }
                if (sureDomainAggregateCache[SURE_VIEW_PATIENT]?.[key] !== undefined) {
                    sureDomainAggregateCache[SURE_VIEW_PATIENT][key] = [];
                }
            });

            // Reset adverse events to all selected
            selectedAdverseEvents = adverseAttributionOptions.map(option => option.value);
            pendingAdverseEvents = [...selectedAdverseEvents];
            populateAdverseEventsDropdown();

            // Reset equity events to all selected
            selectedEquityEvents = adverseAttributionOptions.map(option => option.value);
            pendingEquityEvents = [...selectedEquityEvents];
            populateEquityEventsDropdown();

            if (warningSignsFootnote) {
                warningSignsFootnote.textContent = warningSignsDefaultFootnote;
            }
            updateApplyButtonState();
            updateSureOutcomeToggleStyles();
            updateWaiToggleStyles();
            renderSureOutcomeChart();
            renderWaiUnifiedChart();
            updateDashboard();
        });
    }

    // ============================================================================
    // PATIENT RETENTION BY SESSION
    // ============================================================================
    function updatePatientRetentionChart(query) {
        beginLoading();
        fetch(`/api/patient-retention-by-session?${query}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Failed to load patient retention data');
                }
                return res.json();
            })
            .then(data => {
                if (!Array.isArray(data) || data.length === 0) {
                    createOrUpdateChart('patientRetentionChart', 'bar', { labels: [], datasets: [] }, {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            x: { stacked: true },
                            y: { stacked: true, beginAtZero: true }
                        }
                    });
                    return;
                }

                const labels = data.map(item => `Session ${item.session_id}`);
                const continuing = data.map(item => item.continuing_patients || 0);
                const dropouts = data.map(item => item.dropouts || 0);
                const suicides = data.map(item => item.suicides || 0);
                const totals = data.map(item => item.active_patients || 0);
                const maxPatients = Math.max(...totals, 0);

                createOrUpdateChart('patientRetentionChart', 'bar', {
                    labels,
                    datasets: [
                        {
                            label: 'Continuing Patients',
                            data: continuing,
                            backgroundColor: 'rgba(54, 162, 235, 0.85)',
                            borderColor: 'rgba(54, 162, 235, 1)',
                            borderWidth: 1,
                            stack: 'patients'
                        },
                        {
                            label: 'Dropouts',
                            data: dropouts,
                            backgroundColor: 'rgba(255, 159, 64, 0.8)',
                            borderColor: 'rgba(255, 159, 64, 1)',
                            borderWidth: 1,
                            stack: 'patients'
                        },
                        {
                            label: 'Deaths by Suicide',
                            data: suicides,
                            backgroundColor: 'rgba(220, 53, 69, 0.8)',
                            borderColor: 'rgba(220, 53, 69, 1)',
                            borderWidth: 1,
                            stack: 'patients'
                        }
                    ]
                }, {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: { stacked: true },
                        y: {
                            stacked: true,
                            beginAtZero: true,
                            suggestedMax: maxPatients > 0 ? maxPatients + 1 : undefined,
                            title: { display: true, text: 'Patients' },
                            ticks: { precision: 0 }
                        }
                    },
                    plugins: {
                        legend: { position: 'bottom' },
                        tooltip: {
                            callbacks: {
                                footer: tooltipItems => {
                                    if (!tooltipItems.length) {
                                        return '';
                                    }
                                    const index = tooltipItems[0].dataIndex;
                                    const total = totals[index] || 0;
                                    return `Total Patients: ${total}`;
                                }
                            }
                        }
                    }
                });
            })
            .catch(error => console.error('Error fetching patient retention data:', error))
            .finally(() => endLoading());
    }

    // ============================================================================
    // EQUITY AUDIT (UPDATED)
    // ============================================================================
    function renderEquityCharts(primaryData, stageData, datasetLabel = 'Adverse Outcome Rate (%)') {
        const primaryLabels = Array.isArray(primaryData) ? primaryData.map(item => item.label || 'Unknown') : [];
        const primaryRates = Array.isArray(primaryData) ? primaryData.map(item => {
            const value = Number(item?.rate);
            return Number.isFinite(value) ? value : 0;
        }) : [];

        const stageLabels = Array.isArray(stageData) ? stageData.map(item => item.label || 'Unknown') : [];
        const stageRates = Array.isArray(stageData) ? stageData.map(item => {
            const value = Number(item?.rate);
            return Number.isFinite(value) ? value : 0;
        }) : [];

        createOrUpdateChart('equitySubtypeChart', 'bar', {
            labels: primaryLabels,
            datasets: [{
                label: datasetLabel,
                data: primaryRates,
                backgroundColor: 'rgba(108, 117, 125, 0.7)',
                borderColor: 'rgba(108, 117, 125, 1)',
                borderWidth: 1
            }]
        }, {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    beginAtZero: true,
                    title: { display: true, text: 'Adverse Outcome Rate (%)' }
                }
            },
            plugins: {
                legend: { display: false }
            }
        });

        createOrUpdateChart('equityStageChart', 'bar', {
            labels: stageLabels,
            datasets: [{
                label: datasetLabel,
                data: stageRates,
                backgroundColor: 'rgba(13, 110, 253, 0.7)',
                borderColor: 'rgba(13, 110, 253, 1)',
                borderWidth: 1
            }]
        }, {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Adverse Outcome Rate (%)' }
                }
            },
            plugins: {
                legend: { display: false }
            }
        });
    }

    function aggregateEquityRates(rows, keyFn, labelFn) {
        if (!Array.isArray(rows) || rows.length === 0) {
            return [];
        }

        const totals = new Map();

        rows.forEach(row => {
            const rawKey = keyFn(row);
            const key = rawKey || 'Unknown';
            const rawLabel = labelFn(row, key) || key || 'Unknown';
            const totalSessions = Number(row?.total_sessions) || 0;
            const harmSessions = Number(row?.sessions_with_harm) || 0;

            if (!totals.has(key)) {
                totals.set(key, {
                    key,
                    rawLabel,
                    sessions: 0,
                    harm: 0
                });
            }

            const bucket = totals.get(key);
            if (!bucket.rawLabel && rawLabel) {
                bucket.rawLabel = rawLabel;
            }
            bucket.sessions += totalSessions;
            bucket.harm += harmSessions;
        });

        return Array.from(totals.values()).map(bucket => {
            const sessionCount = bucket.sessions;
            const rawLabel = bucket.rawLabel || 'Unknown';
            const displayLabel = formatSessionsLabel(rawLabel, sessionCount);
            return {
                key: bucket.key,
                rawLabel,
                label: displayLabel,
                sessionCount,
                harmSessions: bucket.harm,
                rate: sessionCount > 0 ? (bucket.harm / sessionCount) * 100 : 0
            };
        });
    }

    function updateEquityViewToggleStyles() {
        if (!equityViewButtons || equityViewButtons.length === 0) {
            return;
        }

        equityViewButtons.forEach(button => {
            const targetView = button.getAttribute('data-equity-view');
            const isActive = targetView === currentEquityView;
            button.classList.toggle('active', isActive);
            button.classList.toggle('btn-primary', isActive);
            button.classList.toggle('btn-outline-light', !isActive);
        });
    }

    function renderEquityAuditView(emptyMessage) {
        updateEquityViewToggleStyles();

        const statsDiv = document.getElementById('equityStats');
        const primaryTitleEl = document.getElementById('equityPrimaryChartTitle');
        const viewDescriptor = currentEquityView === EQUITY_VIEW_THERAPIST ? 'Therapist' : 'Subtype';

        if (primaryTitleEl) {
            primaryTitleEl.textContent = `Adverse Outcome Rates by ${viewDescriptor}`;
        }

        const datasetLabel = 'Adverse Outcome Rate (%)';

        if (!Array.isArray(cachedEquityAuditData) || cachedEquityAuditData.length === 0) {
            renderEquityCharts([], [], datasetLabel);
            if (statsDiv) {
                const message = emptyMessage || cachedEquityEmptyMessage || 'No data available for the selected filters.';
                statsDiv.innerHTML = `<p class="text-muted mb-0">${escapeHtml(message)}</p>`;
            }
            return;
        }

        const primaryData = aggregateEquityRates(
            cachedEquityAuditData,
            row => currentEquityView === EQUITY_VIEW_THERAPIST
                ? (row?.therapist_id || row?.therapist_label || 'Unknown')
                : (row?.subtype_name || 'Unknown'),
            (row, key) => {
                if (currentEquityView === EQUITY_VIEW_THERAPIST) {
                    return row?.therapist_label || getTherapistDisplayName(row?.therapist_id || key) || key || 'Unknown';
                }
                return row?.subtype_name || key || 'Unknown';
            }
        );

        const sortedPrimary = [...primaryData].sort((a, b) => b.rate - a.rate);

        const stageData = aggregateEquityRates(
            cachedEquityAuditData,
            row => row?.state_of_change || 'Unknown',
            (row, key) => key || 'Unknown'
        );

        stageData.sort((a, b) => {
            const orderDiff = (STAGE_DISPLAY_ORDER[a.rawLabel] ?? 999) - (STAGE_DISPLAY_ORDER[b.rawLabel] ?? 999);
            if (orderDiff !== 0) {
                return orderDiff;
            }
            return a.rawLabel.localeCompare(b.rawLabel);
        });

        renderEquityCharts(sortedPrimary, stageData, datasetLabel);

        if (!statsDiv) {
            return;
        }

        const overallTotals = cachedEquityAuditData.reduce((acc, row) => {
            const sessions = Number(row?.total_sessions) || 0;
            const harm = Number(row?.sessions_with_harm) || 0;
            acc.sessions += sessions;
            acc.harm += harm;
            return acc;
        }, { sessions: 0, harm: 0 });

        const overallRate = overallTotals.sessions > 0 ? (overallTotals.harm / overallTotals.sessions) * 100 : 0;
        const highest = sortedPrimary[0];
        const lowest = sortedPrimary[sortedPrimary.length - 1];
        const summaryText = cachedEquityFilterSummary || 'All Adverse Events';
        const highestDescriptor = currentEquityView === EQUITY_VIEW_THERAPIST ? 'Highest Risk Therapist' : 'Highest Risk Group';
        const lowestDescriptor = currentEquityView === EQUITY_VIEW_THERAPIST ? 'Lowest Risk Therapist' : 'Lowest Risk Group';

        const highestLabel = highest ? escapeHtml(highest.label) : '—';
        const highestRateText = highest ? `${highest.rate.toFixed(1)}%` : '—';
        const lowestLabel = lowest ? escapeHtml(lowest.label) : '—';
        const lowestRateText = lowest ? `${lowest.rate.toFixed(1)}%` : '—';

        let statsHtml = `<p class="text-muted small mb-2">Adverse events: ${escapeHtml(summaryText)}</p>`;

        if (sortedPrimary.length > 0) {
            statsHtml += `
                <div class="mb-3">
                    <div class="d-flex justify-content-between mb-2">
                        <span>Overall Harm Rate:</span>
                        <strong>${overallRate.toFixed(1)}%</strong>
                    </div>
                    <div class="d-flex justify-content-between text-danger mb-2">
                        <span>${highestDescriptor}:</span>
                        <strong>${highestLabel}</strong>
                    </div>
                    <div class="d-flex justify-content-between mb-2">
                        <span>Their Harm Rate:</span>
                        <strong>${highestRateText}</strong>
                    </div>
                    <div class="d-flex justify-content-between text-success mb-2">
                        <span>${lowestDescriptor}:</span>
                        <strong>${lowestLabel}</strong>
                    </div>
                    <div class="d-flex justify-content-between">
                        <span>Their Harm Rate:</span>
                        <strong>${lowestRateText}</strong>
                    </div>
                </div>
            `;
        } else {
            statsHtml += '<p class="text-muted mb-0">No data available for the selected filters.</p>';
        }

        statsDiv.innerHTML = statsHtml;
    }

    function setupEquityViewToggle() {
        equityViewButtons = Array.from(document.querySelectorAll('[data-equity-view]'));

        if (!equityViewButtons.length) {
            return;
        }

        equityViewButtons.forEach(button => {
            button.addEventListener('click', () => {
                const targetView = button.getAttribute('data-equity-view');
                if (!targetView || targetView === currentEquityView) {
                    return;
                }
                currentEquityView = targetView;
                updateEquityViewToggleStyles();
                renderEquityAuditView();
            });
        });

        updateEquityViewToggleStyles();
    }

    function updateEquityAudit(baseParams) {
        beginLoading();

        const noEventMessage = 'Select at least one adverse event to view the equity audit.';

        if (selectedEquityEvents.length === 0) {
            cachedEquityAuditData = [];
            cachedEquityFilterSummary = '';
            cachedEquityEmptyMessage = noEventMessage;
            renderEquityAuditView(noEventMessage);
            endLoading();
            return;
        }

        const params = baseParams instanceof URLSearchParams
            ? new URLSearchParams(baseParams.toString())
            : new URLSearchParams(baseParams || '');

        params.delete('equity_event');
        if (selectedEquityEvents.length !== adverseAttributionOptions.length) {
            selectedEquityEvents.forEach(value => params.append('equity_event', value));
        }

        const filterSummary = formatEquitySelectionSummary(selectedEquityEvents);
        cachedEquityFilterSummary = filterSummary;

        fetch(`/api/equity-audit?${params.toString()}`)
            .then(res => {
                if (!res.ok) throw new Error('Failed to load equity audit');
                return res.json();
            })
            .then(data => {
                cachedEquityAuditData = Array.isArray(data) ? data : [];
                cachedEquityEmptyMessage = cachedEquityAuditData.length === 0
                    ? `No data available for ${filterSummary}.`
                    : '';
                renderEquityAuditView();
            })
            .catch(error => {
                console.error('Error fetching equity audit:', error);
                cachedEquityAuditData = [];
                cachedEquityEmptyMessage = 'Failed to load equity audit.';
                renderEquityAuditView('Failed to load equity audit.');
            })
            .finally(() => endLoading());
    }

    function updateScoreTrends(query) {
        beginLoading();
        fetch(`/api/score-trends-over-sessions?${query}`)
            .then(res => {
                if (!res.ok) throw new Error('Failed to load score trends');
                return res.json();
            })
            .then(apiData => {
                let srsTherapistRecords = [];
                let srsPatientRecords = [];
                let waiTherapistRecords = [];
                let waiPatientRecords = [];

                if (Array.isArray(apiData)) {
                    apiData.forEach(row => {
                        const sessionId = row?.session_id;
                        if (row?.avg_srs !== null && row?.avg_srs !== undefined) {
                            srsTherapistRecords.push({
                                session_id: sessionId,
                                therapist_id: 'all',
                                avg_srs: row.avg_srs
                            });
                            srsPatientRecords.push({
                                session_id: sessionId,
                                subtype_name: 'All Patients',
                                avg_srs: row.avg_srs
                            });
                        }
                        if (row?.avg_wai !== null && row?.avg_wai !== undefined) {
                            waiTherapistRecords.push({
                                session_id: sessionId,
                                therapist_id: 'all',
                                avg_wai: row.avg_wai
                            });
                            waiPatientRecords.push({
                                session_id: sessionId,
                                subtype_name: 'All Patients',
                                avg_wai: row.avg_wai
                            });
                        }
                    });
                } else {
                    const srsData = apiData && typeof apiData === 'object' ? apiData.srs : null;
                    const waiData = apiData && typeof apiData === 'object' ? apiData.wai : null;

                    srsTherapistRecords = Array.isArray(srsData?.therapist) ? srsData.therapist : [];
                    srsPatientRecords = Array.isArray(srsData?.patient) ? srsData.patient : [];
                    waiTherapistRecords = Array.isArray(waiData?.therapist) ? waiData.therapist : [];
                    waiPatientRecords = Array.isArray(waiData?.patient) ? waiData.patient : [];
                }

                srsCompositeTrendCache[SRS_VIEW_THERAPIST] = transformSureTrendRecords(
                    srsTherapistRecords,
                    'therapist_id',
                    id => {
                        if (!id || id === 'unknown_therapist') {
                            return 'Unknown Therapist';
                        }
                        if (id === 'all') {
                            return 'All Therapists';
                        }
                        const displayName = getTherapistDisplayName(id);
                        return displayName || 'Unknown Therapist';
                    },
                    'avg_srs'
                );

                srsCompositeTrendCache[SRS_VIEW_PATIENT] = transformSureTrendRecords(
                    srsPatientRecords,
                    'subtype_name',
                    id => {
                        if (!id) {
                            return 'Unknown Subtype';
                        }
                        if (id === 'All Patients') {
                            return 'All Patients';
                        }
                        return id !== 'Unknown' ? id : 'Unknown Subtype';
                    },
                    'avg_srs'
                );

                waiCompositeTrendCache[WAI_ENTITY_THERAPIST] = transformSureTrendRecords(
                    waiTherapistRecords,
                    'therapist_id',
                    id => {
                        if (!id || id === 'unknown_therapist') {
                            return 'Unknown Therapist';
                        }
                        if (id === 'all') {
                            return 'All Therapists';
                        }
                        const displayName = getTherapistDisplayName(id);
                        return displayName || 'Unknown Therapist';
                    },
                    'avg_wai'
                );

                waiCompositeTrendCache[WAI_ENTITY_PATIENT] = transformSureTrendRecords(
                    waiPatientRecords,
                    'subtype_name',
                    id => {
                        if (!id) {
                            return 'Unknown Subtype';
                        }
                        if (id === 'All Patients') {
                            return 'All Patients';
                        }
                        return id !== 'Unknown' ? id : 'Unknown Subtype';
                    },
                    'avg_wai'
                );

                if (currentSrsOutcomeLevel === SRS_OUTCOME_LEVEL_SESSION) {
                    renderSrsOutcomeChart();
                }
                renderWaiUnifiedChart();

            })
            .catch(error => {
                console.error('Error fetching score trends:', error);
                srsCompositeTrendCache[SRS_VIEW_THERAPIST] = createEmptySureTrendCache();
                srsCompositeTrendCache[SRS_VIEW_PATIENT] = createEmptySureTrendCache();
                waiCompositeTrendCache[WAI_ENTITY_THERAPIST] = createEmptySureTrendCache();
                waiCompositeTrendCache[WAI_ENTITY_PATIENT] = createEmptySureTrendCache();
                if (currentSrsOutcomeLevel === SRS_OUTCOME_LEVEL_SESSION) {
                    renderSrsOutcomeChart();
                }
                renderWaiUnifiedChart();
            })
            .finally(() => endLoading());
    }

    // Patient Type Comparisons
    function updatePatientTypeComparisons(query) {
        beginLoading();
        fetch(`/api/scores-by-patient-type?${query}`)
            .then(res => {
                if (!res.ok) throw new Error('Failed to load patient type comparisons');
                return res.json();
            })
            .then(data => {
                if (!data || data.length === 0) {
                    srsCompositeCache[SRS_VIEW_PATIENT] = {
                        labels: [],
                        rawLabels: [],
                        sessionCounts: [],
                        values: []
                    };

                    srsComparisonCache[SRS_VIEW_PATIENT] = {
                        labels: [],
                        rawLabels: [],
                        sessionCounts: [],
                        [SRS_METRIC_OVERALL]: [],
                        [SRS_METRIC_RELATIONSHIP]: [],
                        [SRS_METRIC_GOALS]: [],
                        [SRS_METRIC_APPROACH]: []
                    };

                    if (currentSrsOutcomeView === SRS_VIEW_PATIENT) {
                        renderSrsOutcomeChart();
                    }

                    sureCompositeCache[SURE_VIEW_PATIENT] = {
                        ids: [],
                        labels: [],
                        rawLabels: [],
                        sessionCounts: [],
                        values: []
                    };
                    renderSureOutcomeChart();

                    waiComponentCache[WAI_ENTITY_PATIENT] = {
                        ids: [],
                        labels: [],
                        rawLabels: [],
                        sessionCounts: [],
                        [WAI_METRIC_TASK]: [],
                        [WAI_METRIC_BOND]: [],
                        [WAI_METRIC_GOAL]: []
                    };
                    waiCompositeCache[WAI_ENTITY_PATIENT] = {
                        labels: [],
                        rawLabels: [],
                        sessionCounts: [],
                        values: []
                    };
                    renderWaiUnifiedChart();
                    endLoading();
                    return;
                }

                const subtypes = data.map(d => d.subtype_name);
                const sessionCounts = data.map(d => Number(d.session_count) || 0);
                const displaySubtypes = subtypes.map((name, index) => {
                    let baseLabel;
                    if (!name || name === 'Unknown') {
                        baseLabel = 'Unknown Subtype';
                    } else {
                        baseLabel = name;
                    }
                    return formatSessionsLabel(baseLabel, sessionCounts[index]);
                });
                const normalize = value => {
                    if (value === null || value === undefined) {
                        return 0;
                    }
                    const numeric = Number(value);
                    return Number.isFinite(numeric) ? numeric : 0;
                };

                const srsOverallScores = data.map(d => normalize(d.avg_srs_overall));
                const srsRelationshipScores = data.map(d => normalize(d.avg_srs_relationship));
                const srsGoalsScores = data.map(d => normalize(d.avg_srs_goals));
                const srsApproachScores = data.map(d => normalize(d.avg_srs_approach));
                const waiScores = data.map(d => normalize(d.avg_wai));
                const waiTaskScores = data.map(d => normalize(d.avg_wai_task));
                const waiBondScores = data.map(d => normalize(d.avg_wai_bond));
                const waiGoalScores = data.map(d => normalize(d.avg_wai_goal));
                const neqScores = data.map(d => (d.avg_neq === null || d.avg_neq === undefined)
                    ? null
                    : Number(d.avg_neq));

                const toNumericOrNull = value => (value === null || value === undefined) ? null : Number(value);
                const srsCompositeValues = data.map(d => toNumericOrNull(d.avg_srs));
                const sureCompositeValues = data.map(d => toNumericOrNull(d.avg_sure));

                srsCompositeCache[SRS_VIEW_PATIENT] = {
                    labels: displaySubtypes.slice(),
                    rawLabels: subtypes.slice(),
                    sessionCounts: sessionCounts.slice(),
                    values: srsCompositeValues.slice()
                };

                srsComparisonCache[SRS_VIEW_PATIENT] = {
                    labels: displaySubtypes.slice(),
                    rawLabels: subtypes.slice(),
                    sessionCounts: sessionCounts.slice(),
                    [SRS_METRIC_OVERALL]: srsOverallScores.map(toNumericOrNull),
                    [SRS_METRIC_RELATIONSHIP]: srsRelationshipScores.map(toNumericOrNull),
                    [SRS_METRIC_GOALS]: srsGoalsScores.map(toNumericOrNull),
                    [SRS_METRIC_APPROACH]: srsApproachScores.map(toNumericOrNull)
                };

                if (currentSrsOutcomeView === SRS_VIEW_PATIENT) {
                    renderSrsOutcomeChart();
                }

                sureCompositeCache[SURE_VIEW_PATIENT] = {
                    ids: subtypes.slice(),
                    labels: displaySubtypes.slice(),
                    rawLabels: subtypes.slice(),
                    sessionCounts: sessionCounts.slice(),
                    values: sureCompositeValues.slice()
                };

                renderSureOutcomeChart();

                waiCompositeCache[WAI_ENTITY_PATIENT] = {
                    labels: displaySubtypes.slice(),
                    rawLabels: subtypes.slice(),
                    sessionCounts: sessionCounts.slice(),
                    values: waiScores.map(value => (value === null || value === undefined) ? null : Number(value))
                };

                waiComponentCache[WAI_ENTITY_PATIENT] = {
                    ids: [],
                    labels: displaySubtypes.slice(),
                    rawLabels: subtypes.slice(),
                    sessionCounts: sessionCounts.slice(),
                    [WAI_METRIC_TASK]: waiTaskScores.slice(),
                    [WAI_METRIC_BOND]: waiBondScores.slice(),
                    [WAI_METRIC_GOAL]: waiGoalScores.slice()
                };

                renderWaiUnifiedChart();

                const patientKeys = subtypes.map(name => {
                    if (!name || name === 'Unknown') {
                        return 'Unknown Subtype';
                    }
                    return name;
                });

                const uniquePatientKeys = [];
                patientKeys.forEach((key, index) => {
                    const label = displaySubtypes[index] || subtypes[index] || 'Unknown Subtype';
                    setNeqMetric(NEQ_VIEW_PATIENT, key, label, NEQ_METRIC_SEVERITY, neqScores[index]);
                    if (!uniquePatientKeys.includes(key)) {
                        uniquePatientKeys.push(key);
                    }
                });

                const existingPatientOrder = neqInterventionOrder[NEQ_VIEW_PATIENT]
                    .filter(key => !uniquePatientKeys.includes(key));
                neqInterventionOrder[NEQ_VIEW_PATIENT] = [...uniquePatientKeys, ...existingPatientOrder];

                if (currentNeqMode === NEQ_MODE_INTERVENTION
                    && currentNeqView === NEQ_VIEW_PATIENT
                    && currentNeqMetric === NEQ_METRIC_SEVERITY) {
                    renderNeqUnifiedChart();
                }
            })
            .catch(error => console.error('Error fetching patient type comparisons:', error))
            .finally(() => endLoading());
    }

    // --- START THE APPLICATION ---
    initialize();
});