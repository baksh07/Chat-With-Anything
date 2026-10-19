from openai import OpenAI

# Initialize the client
client = OpenAI(
    api_key="sl-llm",
    base_url="http://216.48.184.150:3000/"
)

try:
    # Make the chat completion request with streaming
    chat_completion = client.chat.completions.create(
        messages=[
        {
        "query": "what is the chat about",
        "noteRef": ["352", "324"],
        "chat_id": "1",
        "uniqueId": [
            "fe34dde3-bd8f-4823-90b0-2adece6820f9",
            "709abc9d-5db7-4118-b9db-de6677369db7"
        ]
        }
    ],
        model="sl-llm",
        stream=True,
        temperature=0.7,  # Optional: controls randomness
        max_tokens=1000   # Optional: limits response length
    )

    # Process the streaming response
    for chunk in chat_completion:
        if chunk.choices[0].delta.content is not None:
            print(chunk.choices[0].delta.content, end="", flush=True)

except Exception as e:
    print(f"An error occurred: {str(e)}")



