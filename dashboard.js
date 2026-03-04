document.addEventListener('DOMContentLoaded', () => {
    
    // Auth Check
    const token = localStorage.getItem('apex_ai_token');
    if (!token) {
        // Redirect back if not logged in
        window.location.href = 'index.html';
        return;
    }

    // Auto-resize textarea
    const textarea = document.getElementById('chat-textarea');
    textarea.addEventListener('input', function() {
        this.style.height = 'auto';
        this.style.height = (this.scrollHeight) + 'px';
        if (this.value === '') {
            this.style.height = 'auto';
        }
    });

    // Enter to Send
    textarea.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            document.getElementById('dashboard-chat-form').dispatchEvent(new Event('submit'));
        }
    });

    const form = document.getElementById('dashboard-chat-form');
    const messagesContainer = document.getElementById('chat-messages');

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const msg = textarea.value.trim();
        if (!msg) return;

        textarea.value = '';
        textarea.style.height = 'auto';

        // Hide welcome screen if it exists
        const welcome = document.querySelector('.welcome-screen');
        if (welcome) welcome.style.display = 'none';

        // Add User Message
        appendMessage('user', msg);

        // Fetch AI Response 
        // Real implementation hits `/api/v1/ai/chat` 
        const selectedModel = document.getElementById('ai-model-select').value;
        await fetchAIResponse(msg, selectedModel);
    });

    function appendMessage(sender, text) {
        const div = document.createElement('div');
        div.classList.add('message', sender);
        
        let avatarHtml = '';
        if (sender === 'ai') {
            avatarHtml = `<div class="msg-avatar">A</div>`;
        }

        div.innerHTML = `
            ${avatarHtml}
            <div class="msg-content">
                <div class="msg-text">${text}</div>
            </div>
        `;
        
        // Before inserting, if it's AI, make it look nice
        messagesContainer.appendChild(div);
        messagesContainer.scrollTop = messagesContainer.scrollHeight;
        return div;
    }

    async function fetchAIResponse(message, model) {
        const loadingDiv = appendMessage('ai', '<span class="pulse-loader">Thinking...</span>');
        
        try {
            const res = await fetch('/api/v1/ai/chat', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${token}` // use verified JWT 
                },
                body: JSON.stringify({ message: message, model: model, language: 'uz' })
            });

            if(!res.ok) throw new Error('Failed to reach models. Server active?');
            const data = await res.json();
            
            // Replace loading with real text
            loadingDiv.querySelector('.msg-text').innerHTML = parseMarkdown(data.content);

        } catch (err) {
            loadingDiv.querySelector('.msg-text').innerHTML = `<span style="color:#ff4d4d;">Xatolik yuz berdi: Orqa server ulanmagan yoki API cheklovi.</span>`;
        }
    }

    // Basic markdown parser for responses
    function parseMarkdown(text) {
        return text
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.*?)\*/g, '<em>$1</em>')
            .replace(/`(.*?)`/g, '<code style="background:rgba(255,255,255,0.1); padding:2px 4px; border-radius:4px;">$1</code>')
            .replace(/\n/g, '<br>');
    }

    // Logout
    document.getElementById('logout-btn').addEventListener('click', (e) => {
        e.preventDefault();
        localStorage.removeItem('apex_ai_token');
        window.location.href = 'index.html';
    });

    // Populate user details via token parse (Simple client side decode)
    try {
        const payload = JSON.parse(atob(token.split('.')[1]));
        document.getElementById('user-name-display').innerText = payload.sub || "Mijoz";
    } catch(e) {}
});
