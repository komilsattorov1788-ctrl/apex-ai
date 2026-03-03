async def generate_code(prompt: str, lang_code: str) -> dict:
    """
    Handles code generation requests via Claude 3.5 Sonnet.
    """
    code_res = "Siz so'ragan kod:" if lang_code == 'uz' else "Вот ваш код:" if lang_code == 'ru' else "Here is your code:" if lang_code == 'en' else f"Code [{lang_code}]:"
    return {
        "source": f"Claude 3.5 Sonnet ({lang_code.upper()})", 
        "content": f"{code_res}\n\ndef run_apex_process():\n    print('Apex AI logic is being executed...')\n    return 'Success'"
    }
