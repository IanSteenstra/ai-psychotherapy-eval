"""Example therapist implemented as a Python function.

Point a config at it with:

    therapists:
      - id: my_therapist
        type: python
        function: examples.my_therapist:respond

Replace the body with a call to your own system (a local model, an agent framework,
an SDK, ...). The function may be sync or async and must return the reply text.
"""

from aipsycheval import TherapistRequest


def respond(request: TherapistRequest) -> str:
    # request.patient_message        latest patient message
    # request.history                this session: [{"role": "patient" | "therapist", "content": "..."}]
    # request.openai_messages()      the same history as OpenAI-style user/assistant messages
    # request.previous_sessions      earlier sessions in the same format
    # request.pairing_id / session_id / turn / patient_id for bookkeeping
    if request.turn == 1:
        return "Hi, I'm glad you came in today. What would you like to talk about?"
    return f"It sounds like you're saying: \"{request.patient_message[:80]}\". Can you tell me more about that?"
