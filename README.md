# ai-psychotherapy-eval

An Evaluation Framework for Assessing Quality of Care & Risk in AI Psychotherapy.

## Prerequisites

*   **Python 3.12** is required to run this project.

## Installation

1.  Clone the repository:
    ```bash
    git clone <repository-url>
    cd ai-psychotherapy-eval
    ```

## Running the Simulation

The simulation runner executes the AI psychotherapy sessions and logs the data. It requires its own virtual environment.

1.  Navigate to the `run_simulation` directory:
    ```bash
    cd run_simulation
    ```

2.  Create and activate the simulation environment:
    ```bash
    python3.12 -m venv .sim_env
    source .sim_env/bin/activate
    ```

3.  Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configuration**:
    Open `simulation_runner.py` and update the `Config` class with your API keys:
    *   `GEMINI_API_KEY`
    *   `OPENAI_API_KEY`
    *   `CHARACTERAI_API_KEY`
    *   `CHARACTERAI_ID`

5.  Run the simulation:
    ```bash
    python simulation_runner.py
    ```

6.  Deactivate the environment when done:
    ```bash
    deactivate
    cd ..
    ```

## Uploading Data to BigQuery

The interactive dashboard reads data from Google BigQuery. After running the simulation, you must upload the generated logs.

**Prerequisites:**
*   Google Cloud SDK (`gcloud` and `bq` CLI tools) installed and authenticated.
*   A Google Cloud Platform (GCP) project.

1.  Navigate to the `run_simulation` directory (if not already there):
    ```bash
    cd run_simulation
    ```

2.  **Configuration**:
    Open `populate_all_tables.sh` and update the `PROJECT_ID` variable with your GCP project ID:
    ```bash
    PROJECT_ID="<your-gcp-project-id>"
    ```

3.  Run the upload script:
    ```bash
    chmod +x populate_all_tables.sh
    ./populate_all_tables.sh
    ```

    This script creates the necessary dataset and tables in BigQuery and uploads the CSV logs from the `logs/` directory.

## Running the Interactive Dashboard

The dashboard allows you to visualize and analyze the simulation results. It is designed to be deployed on **Google App Engine**, but can be run locally. It requires a separate virtual environment.

1.  Navigate to the `interactive_dashboard` directory:
    ```bash
    cd interactive_dashboard
    ```

2.  Create and activate the dashboard environment:
    ```bash
    python3.12 -m venv .dash_env
    source .dash_env/bin/activate
    ```

3.  Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

4.  (Optional) Configure Redis for caching:
    Set the following environment variables if you have a Redis instance:
    *   `REDIS_URL` or `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`

5.  Run the dashboard application:
    ```bash
    python main.py
    ```

6.  Open your browser and navigate to:
    `http://localhost:8080`
