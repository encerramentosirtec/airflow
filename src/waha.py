import requests

class Waha:

    def __init__(self):
        self.header = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Api-Key": "8936ee414309428ca1ba6065a207ad37"
        }

    def send_private_message(self, chat_id, text, session="default"):
        url = "http://localhost:3000/api/sendText"
        headers = self.header
        data = {
            "chatId": f'{chat_id}@c.us',
            "text": text,
            "session": session
        }
        response = requests.post(url, json=data, headers=headers)
        return response.json()
    
    def send_group_message(self, chat_id, text, mentions=None, session="default"):
        url = "http://localhost:3000/api/sendText"
        headers = self.header
        data = {
            "chatId": f'{chat_id}@g.us',
            "text": text,
            "session": session,
            "mentions": mentions
        }
        response = requests.post(url, json=data, headers=headers)
        return response.json()


 