async def get_default_chat_response(prompt: str, lang_code: str) -> dict:
    """
    Standard text conversational AI response based on language.
    """
    responses = {
        'uz': "Assalomu alaykum! Men Apex AI – dunyodagi 150 dan ortiq tilda erkin muloqot qilaman. Bugun sizga qanday yordam kerak?",
        'ru': "Здравствуйте! Я Apex AI, и я работаю на более чем 150 языках мира без потери качества. Чем могу помочь?",
        'en': "Hello! I am Apex AI, operating natively in over 150 globally recognized languages. How can I assist?",
        'tr': "Merhaba! Ben 150'den fazla dilde serbestçe iletişim kuran Apex AI'yım. Size nasıl yardımcı olabilirim?",
        'es': "¡Hola! Soy Apex AI y me comunico con fluidez en más de 150 idiomas. ¿En qué te puedo ayudar hoy?",
        'zh-cn': "你好！我是Apex AI，可以流利地使用150多门语言。今天我能为你做点什么？",
        'fr': "Bonjour ! Je suis Apex AI. Je peux communiquer dans plus de 150 langues. Comment puis-je vous aider aujourd'hui?",
        'de': "Hallo! Ich bin Apex AI. Ich kann in über 150 Sprachen kommunizieren. Wie kann ich heute helfen?",
        'ja': "こんにちは！私はApex AIです。世界150カ国語以上の言語で完璧にコミュニケーションが取れます。ご用件は何でしょうか？",
        'ar': "مرحباً! أنا Apex AI أدعم أكثر من 150 لغة. كيف يمكنني مساعدتك؟"
    }
    
    final_response = responses.get(lang_code, f"Sizning tilingiz (Til kodi: {lang_code.upper()}) muvaffaqiyatli aniqlandi! Men Apex AI tizimiman, API (OpenAI) ulangandan sushng bu tilda erkin javob qaytaraman.")
    
    return {
        "source": f"GPT-4o ({lang_code.upper()})", 
        "content": final_response
    }
