async def echo(request):
    return f"You said: {request.patient_message}"
