import requests
import json

class LingoVoiceProcessor:
    def __init__(self, base_url):
        self.base_url = base_url
        self.headers = {
            'Content-Type': 'application/json'
        }

    def process_text(self, text, summarization=True):
        url = "http://216.48.184.150:3999/LingoVoice/process_text/?summarization=true"
        payload = json.dumps({
            "Text": text
            })
        headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
        }
        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            return {
                "status_code": response.status_code,
                "response": response.json()
            }
        except requests.exceptions.RequestException as e:
            return {
                "status_code": None,
                "error": str(e)
            }
        except json.JSONDecodeError:
            return {
                "status_code": response.status_code,
                "error": "Invalid JSON response",
                "raw_response": response.text
            }


    def process_audio_url(self, audio_url, summarization=True):
        url = self.base_url
        if summarization:
            url += "?summarization=true"

        payload = {
            "audiourl": audio_url
        }
        url2 = "http://localhost:5999/LingoVoice/process_audio_url/?summarization=True&title=true"
        try:
            response = requests.post(url2, headers=self.headers, data=json.dumps(payload))
            return {
                "status_code": response.status_code,
                "response": response.json()
            }
        except requests.exceptions.RequestException as e:
            return {
                "status_code": None,
                "error": str(e)
            }
        except json.JSONDecodeError:
            return {
                "status_code": response.status_code,
                "error": "Invalid JSON response",
                "raw_response": response.text
            }

# Example usage
if __name__ == "__main__":
    processor = LingoVoiceProcessor("http://216.48.184.150:5999/LingoVoice/process_audio_url/")
    audio_url = "https://lingoscaledata.s3.us-east-2.amazonaws.com/test/9bbb2268-cfb5-481f-9745-4f656e8ed40e.mp3?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=AKIA6ODU3R7HODSP7XRK%2F20250619%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20250619T105152Z&X-Amz-Expires=82400&X-Amz-Signature=bf10074e6be84fdec53a8d620ca1e85259d3914d1e931469b9e0b77317578a4e&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject"
    result = processor.process_audio_url(audio_url)
    print(result)
