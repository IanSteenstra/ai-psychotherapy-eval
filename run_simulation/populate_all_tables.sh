#!/bin/bash

PROJECT_ID="<gcp-project-id>"
DATASET="simulation_logs"
LOG_DIR="./logs"

echo "=========================================="
echo "BigQuery Table Upload Script"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET"
echo ""

# Set project
gcloud config set project $PROJECT_ID

# Ensure dataset exists
echo "Creating dataset (if it doesn't exist)..."
bq mk --dataset --location=US "$PROJECT_ID:$DATASET" 2>/dev/null || echo "Dataset already exists"

# Ensure a partitioned/clustering table exists before loading
ensure_table() {
    local table_name=$1
    local schema=$2
    local partition_range=$3
    local clustering=$4

    if bq show --format=none "$PROJECT_ID:$DATASET.$table_name" >/dev/null 2>&1; then
        echo "Table $table_name already exists; preserving partitioning and clustering"
        return 0
    fi

    echo "Creating table $table_name with configured partitioning/clustering..."

    local cmd=(
        bq mk
        --table
    )

    if [ -n "$partition_range" ]; then
        cmd+=(--range_partitioning="$partition_range")
    fi

    if [ -n "$clustering" ]; then
        cmd+=(--clustering_fields="$clustering")
    fi

    cmd+=("$PROJECT_ID:$DATASET.$table_name" "$schema")

    if "${cmd[@]}"; then
        echo "✓ Created $table_name"
    else
        echo "✗ Failed to create $table_name"
        return 1
    fi
}

# Simple load function with append/replace support
load_table() {
    local table_name=$1
    local schema=$2
    local csv_file="$LOG_DIR/$table_name.csv"
    local clustering=$3
    local partition_range=$4
    local mode=${5:-append}
    
    echo ""
    echo "------------------------------------"
    echo "Loading: $table_name"
    echo "------------------------------------"
    
    if [ ! -f "$csv_file" ]; then
        echo "ERROR: File not found: $csv_file"
        return 1
    fi
    
    local line_count=$(wc -l < "$csv_file" | tr -d ' ')
    echo "Rows in CSV: $((line_count - 1))"
    
    # Delete existing table if clustering will be applied
    if [ "$mode" != "replace" ] && { [ -n "$partition_range" ] || [ -n "$clustering" ]; }; then
        ensure_table "$table_name" "$schema" "$partition_range" "$clustering"
    fi
    
    # Build command
    local cmd=(
        bq load
        --source_format=CSV
        --skip_leading_rows=1
        --allow_quoted_newlines
    )

    if [ "$mode" = "replace" ]; then
        cmd+=(--replace)
        if [ -n "$clustering" ]; then
            echo "Clustering by: $clustering"
            cmd+=(--clustering_fields="$clustering")
        fi
        if [ -n "$partition_range" ]; then
            echo "Applying range partitioning: $partition_range"
            cmd+=(--range_partitioning="$partition_range")
        fi
    else
        cmd+=(--noreplace)
    fi
    
    cmd+=(
        "$PROJECT_ID:$DATASET.$table_name"
        "$csv_file"
        "$schema"
    )
    
    # Execute
    if "${cmd[@]}"; then
        echo "✓ Successfully loaded $table_name"
        
        if [ "$mode" = "replace" ]; then
            local loaded_count=$(bq query --use_legacy_sql=false --format=csv \
                "SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET.$table_name\`" 2>/dev/null | tail -1)
            local expected_count=$((line_count - 1))

            if [ "$loaded_count" != "$expected_count" ]; then
                echo "⚠ WARNING: Expected $expected_count rows but loaded $loaded_count rows"
                echo "  Check for rejected rows or schema mismatches"
            else
                echo "  Verified: $loaded_count rows loaded"
            fi
        fi
    else
        echo "✗ Failed to load $table_name"
        return 1
    fi
}

# Load metadata tables
echo ""
echo "========================================"
echo "Loading Metadata Tables"
echo "========================================"

load_table "patient_personas" \
    "patient_id:INTEGER,name:STRING,subtype_name:STRING,ad_subtype_description:STRING,age_onset:STRING,aud_severity_symptoms:STRING,drinking_pattern:STRING,family_history_of_alcohol_dependence:STRING,antisocial_personality_disorder:STRING,comorbid_psychiatric_disorders:STRING,comorbid_substance_use:STRING,psychosocial_indicators:STRING,help_seeking_behavior:STRING,state_of_change:STRING,persona_description:STRING,hopelessness_intensity:INTEGER,negative_core_belief_intensity:INTEGER,cognitive_preoccupation_with_use_intensity:INTEGER,self_efficacy_intensity:INTEGER,distress_tolerance_intensity:INTEGER,substance_craving_intensity:INTEGER,motivational_intensity:INTEGER,ambivalence_about_change_intensity:INTEGER,perceived_burdensomeness_intensity:INTEGER,thwarted_belongingness_intensity:INTEGER" \
    "" "" "replace"

load_table "simulation_pairings" \
    "pairing_id:INTEGER,therapist_id:STRING,patient_id:INTEGER" \
    "" "" "replace"

# Load log tables with clustering
echo ""
echo "========================================"
echo "Loading Log Tables"
echo "========================================"

load_table "crisis_eval_logs" \
    "pairing_id:INTEGER,session_id:INTEGER,turn:INTEGER,reasoning:STRING,classification:STRING" \
    "session_id" \
    "pairing_id,0,100000,100" \
    "append"

load_table "survey_wai_logs" \
    "pairing_id:INTEGER,session_id:INTEGER,question1:STRING,question2:STRING,question3:STRING,question4:STRING,question5:STRING,question6:STRING,question7:STRING,question8:STRING,question9:STRING,question10:STRING,question11:STRING,question12:STRING,question13:STRING,question14:STRING,question15:STRING,question16:STRING,question17:STRING,question18:STRING,question19:STRING,question20:STRING,question21:STRING,question22:STRING,question23:STRING,question24:STRING,question25:STRING,question26:STRING,question27:STRING,question28:STRING,question29:STRING,question30:STRING,question31:STRING,question32:STRING,question33:STRING,question34:STRING,question35:STRING,question36:STRING,total_wai_task:INTEGER,total_wai_bond:INTEGER,total_wai_goal:INTEGER,composite_wai:INTEGER" \
    "session_id" \
    "pairing_id,0,100000,100" \
    "append"

load_table "survey_sure_logs" \
    "pairing_id:INTEGER,session_id:INTEGER,sec_a_question_1:STRING,sec_a_question_2:STRING,sec_a_question_3:STRING,sec_a_question_4:STRING,sec_a_question_5:STRING,sec_a_question_6:STRING,sec_b_question_7:STRING,sec_b_question_8:STRING,sec_b_question_9:STRING,sec_b_question_10:STRING,sec_b_question_11:STRING,sec_b_question_12:STRING,sec_b_question_13:STRING,sec_b_question_14:STRING,sec_b_question_15:STRING,sec_b_question_16:STRING,sec_b_question_17:STRING,sec_b_question_18:STRING,sec_b_question_19:STRING,sec_b_question_20:STRING,sec_b_question_21:STRING,sec_c_question_1:STRING,sec_c_question_2:STRING,sec_c_question_3:STRING,sec_c_question_4:STRING,sec_c_question_5:STRING,total_sure_drug_use:INTEGER,total_sure_self_care:INTEGER,total_sure_relationships:INTEGER,total_sure_material_resources:INTEGER,total_sure_outlook:INTEGER,total_sure_score:INTEGER" \
    "session_id" \
    "pairing_id,0,100000,100" \
    "append"

load_table "survey_srs_logs" \
    "pairing_id:INTEGER,session_id:INTEGER,relationship:FLOAT,goals_and_topics:FLOAT,approach_or_method:FLOAT,overall:FLOAT" \
    "session_id" \
    "pairing_id,0,100000,100" \
    "append"

load_table "survey_neq_logs" \
    "pairing_id:INTEGER,session_id:INTEGER,question1_experienced:BOOLEAN,question1_severity:STRING,question1_cause:STRING,question2_experienced:BOOLEAN,question2_severity:STRING,question2_cause:STRING,question3_experienced:BOOLEAN,question3_severity:STRING,question3_cause:STRING,question4_experienced:BOOLEAN,question4_severity:STRING,question4_cause:STRING,question5_experienced:BOOLEAN,question5_severity:STRING,question5_cause:STRING,question6_experienced:BOOLEAN,question6_severity:STRING,question6_cause:STRING,question7_experienced:BOOLEAN,question7_severity:STRING,question7_cause:STRING,question8_experienced:BOOLEAN,question8_severity:STRING,question8_cause:STRING,question9_experienced:BOOLEAN,question9_severity:STRING,question9_cause:STRING,question10_experienced:BOOLEAN,question10_severity:STRING,question10_cause:STRING,question11_experienced:BOOLEAN,question11_severity:STRING,question11_cause:STRING,question12_experienced:BOOLEAN,question12_severity:STRING,question12_cause:STRING,question13_experienced:BOOLEAN,question13_severity:STRING,question13_cause:STRING,question14_experienced:BOOLEAN,question14_severity:STRING,question14_cause:STRING,question15_experienced:BOOLEAN,question15_severity:STRING,question15_cause:STRING,question16_experienced:BOOLEAN,question16_severity:STRING,question16_cause:STRING,question17_experienced:BOOLEAN,question17_severity:STRING,question17_cause:STRING,question18_experienced:BOOLEAN,question18_severity:STRING,question18_cause:STRING,question19_experienced:BOOLEAN,question19_severity:STRING,question19_cause:STRING,question20_experienced:BOOLEAN,question20_severity:STRING,question20_cause:STRING,question21_experienced:BOOLEAN,question21_severity:STRING,question21_cause:STRING,question22_experienced:BOOLEAN,question22_severity:STRING,question22_cause:STRING,question23_experienced:BOOLEAN,question23_severity:STRING,question23_cause:STRING,question24_experienced:BOOLEAN,question24_severity:STRING,question24_cause:STRING,question25_experienced:BOOLEAN,question25_severity:STRING,question25_cause:STRING,question26_experienced:BOOLEAN,question26_severity:STRING,question26_cause:STRING,question27_experienced:BOOLEAN,question27_severity:STRING,question27_cause:STRING,question28_experienced:BOOLEAN,question28_severity:STRING,question28_cause:STRING,question29_experienced:BOOLEAN,question29_severity:STRING,question29_cause:STRING,question30_experienced:BOOLEAN,question30_severity:STRING,question30_cause:STRING,question31_experienced:BOOLEAN,question31_severity:STRING,question31_cause:STRING,question32_experienced:BOOLEAN,question32_severity:STRING,question32_cause:STRING,other_incidents_or_effects:STRING,neq_total_effects_experienced:INTEGER,neq_effects_due_to_treatment:INTEGER,neq_effects_due_to_other:INTEGER,neq_total_severity_score:INTEGER,neq_avg_severity_of_experienced_effects:FLOAT" \
    "session_id" \
    "pairing_id,0,100000,100" \
    "append"

load_table "mi_global_eval_logs" \
    "pairing_id:INTEGER,session_id:INTEGER,cultivating_change_talk_score:INTEGER,cultivating_change_talk_reasoning:STRING,softening_sustain_talk_score:INTEGER,softening_sustain_talk_reasoning:STRING,partnership_score:INTEGER,partnership_reasoning:STRING,empathy_score:INTEGER,empathy_reasoning:STRING" \
    "session_id" \
    "pairing_id,0,100000,100" \
    "append"

load_table "mi_batch_behavior_eval_logs" \
    "pairing_id:INTEGER,session_id:INTEGER,reasoning:STRING,GI:INTEGER,Persuade:INTEGER,Persuade_with:INTEGER,Q:INTEGER,SR:INTEGER,CR:INTEGER,AF:INTEGER,Seek:INTEGER,Emphasize:INTEGER,Confront:INTEGER,total_mi_adherent:INTEGER,total_mi_non_adherent:INTEGER,percent_mi_adherent:FLOAT,percent_cr:FLOAT,r_q_ratio:FLOAT" \
    "session_id" \
    "pairing_id,0,100000,100" \
    "append"

load_table "conversation_log" \
    "pairing_id:INTEGER,session_id:INTEGER,turn:INTEGER,speaker:STRING,message:STRING,session_conclusion:BOOLEAN,appraisal_internal_reflection:STRING,internal_justification:STRING,goal:STRING,strategy:STRING,tactic:STRING,hopelessness_intensity:FLOAT,negative_core_belief_intensity:FLOAT,cognitive_preoccupation_with_use_intensity:FLOAT,self_efficacy_intensity:FLOAT,distress_tolerance_intensity:FLOAT,substance_craving_intensity:FLOAT,motivational_intensity:FLOAT,ambivalence_about_change_intensity:FLOAT,perceived_burdensomeness_intensity:FLOAT,thwarted_belongingness_intensity:FLOAT" \
    "session_id,turn" \
    "pairing_id,0,100000,100" \
    "append"

load_table "after_session_reports" \
    "pairing_id:INTEGER,session_id:INTEGER,journal_summary:STRING,state_change_justification:STRING,death_by_suicide_occurred:BOOLEAN,death_by_suicide_attribution:STRING,death_by_suicide_internal_justification:STRING,suicide_attempt_occurred:BOOLEAN,suicide_attempt_attribution:STRING,suicide_attempt_internal_justification:STRING,non_suicidal_self_injury_occurred:BOOLEAN,non_suicidal_self_injury_attribution:STRING,non_suicidal_self_injury_internal_justification:STRING,relapse_substance_use_occurred:BOOLEAN,relapse_substance_use_attribution:STRING,relapse_substance_use_internal_justification:STRING,increase_alcohol_seeking_occurred:BOOLEAN,increase_alcohol_seeking_attribution:STRING,increase_alcohol_seeking_internal_justification:STRING,neglect_of_roles_occurred:BOOLEAN,neglect_of_roles_attribution:STRING,neglect_of_roles_internal_justification:STRING,treatment_dropout_occurred:BOOLEAN,treatment_dropout_attribution:STRING,treatment_dropout_internal_justification:STRING,intensification_suicidal_ideation_occurred:BOOLEAN,intensification_suicidal_ideation_attribution:STRING,intensification_suicidal_ideation_internal_justification:STRING,intensification_shame_stigma_occurred:BOOLEAN,intensification_shame_stigma_attribution:STRING,intensification_shame_stigma_internal_justification:STRING,interpersonal_functioning_decline_occurred:BOOLEAN,interpersonal_functioning_decline_attribution:STRING,interpersonal_functioning_decline_internal_justification:STRING,hopelessness_intensity:INTEGER,negative_core_belief_intensity:INTEGER,cognitive_preoccupation_with_use_intensity:INTEGER,self_efficacy_intensity:INTEGER,distress_tolerance_intensity:INTEGER,substance_craving_intensity:INTEGER,motivational_intensity:INTEGER,ambivalence_about_change_intensity:INTEGER,perceived_burdensomeness_intensity:INTEGER,thwarted_belongingness_intensity:INTEGER" \
    "session_id" \
    "pairing_id,0,100000,100" \
    "append"

load_table "action_plan_eval_logs" \
    "pairing_id:INTEGER,session_id:INTEGER,turn:INTEGER,assess:BOOLEAN,de_escalate:BOOLEAN,recommend_emergency_services:BOOLEAN,request_human_consultation:BOOLEAN,overall_justification:STRING" \
    "session_id" \
    "pairing_id,0,100000,100" \
    "append"

echo ""
echo "=========================================="
echo "Upload Complete!"
echo "=========================================="
echo ""