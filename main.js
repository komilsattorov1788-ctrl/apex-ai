document.addEventListener('DOMContentLoaded', () => {
    // Interactive Glow Effect for Cards
    const cards = document.querySelectorAll('.feature-card');

    document.getElementById('features').addEventListener('mousemove', e => {
        for (const card of cards) {
            const rect = card.getBoundingClientRect(),
                x = e.clientX - rect.left,
                y = e.clientY - rect.top;

            card.style.setProperty('--mouse-x', `${x}px`);
            card.style.setProperty('--mouse-y', `${y}px`);
        }
    });

    // Theme Switcher Logic for Preview
    window.setTheme = (themeName) => {
        document.body.className = themeName === 'default' ? '' : `theme-${themeName}`;
        console.log(`Theme switched to: ${themeName}`);
    };

    // Advanced Parallax & Mouse Tracking
    document.addEventListener('mousemove', (e) => {
        const x = (window.innerWidth / 2 - e.pageX) / 45;
        const y = (window.innerHeight / 2 - e.pageY) / 45;
        
        document.documentElement.style.setProperty('--move-x', `${x}deg`);
        document.documentElement.style.setProperty('--move-y', `${y}deg`);
    });

    // Sentinel Live Logging
    const sentinelLogs = document.getElementById('sentinel-logs');
    if(sentinelLogs) {
        const logMsgs = [
            {t: "[SEC] AES-256 Encryption handshake...", c: "#00f0ff"},
            {t: "[API] Routing to GPT-5 Cluster...", c: "#27c93f"},
            {t: "[DB] Idempotency lock acquired.", c: "#888"},
            {t: "[RAG] Scraping Google Search Node...", c: "#ffbd2e"},
            {t: "[SYS] Load Balancer: Traffic rerouted.", c: "#00f0ff"},
            {t: "[AI] Analyzing Consensus from 4 nodes...", c: "#b026ff"}
        ];
        setInterval(() => {
            const msg = logMsgs[Math.floor(Math.random() * logMsgs.length)];
            const div = document.createElement('div');
            div.style.color = msg.c;
            div.innerText = msg.t;
            sentinelLogs.prepend(div);
            if(sentinelLogs.children.length > 8) sentinelLogs.lastElementChild.remove();
        }, 3000);
    }

    // Smart Navbar Background
    const navbar = document.querySelector('.navbar');
    window.addEventListener('scroll', () => {
        if (window.scrollY > 50) {
            navbar.style.background = 'rgba(5, 5, 5, 0.85)';
            navbar.style.boxShadow = '0 10px 30px rgba(0, 0, 0, 0.5)';
        } else {
            navbar.style.background = 'rgba(5, 5, 5, 0.6)';
            navbar.style.boxShadow = 'none';
        }
    });

    // Interactive AI Terminal Simulation connected to Backend
    const terminalBody = document.getElementById('terminal-body');
    const terminalInputArea = document.getElementById('terminal-input-form');
    const terminalInput = document.getElementById('terminal-user-input');

    async function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async function typeText(container, text, step = 30) {
        let cursor = document.createElement('span');
        cursor.className = 'typing-cursor';
        container.appendChild(cursor);

        for (let j = 0; j < text.length; j++) {
            container.insertBefore(document.createTextNode(text[j]), cursor);
            await sleep(step);
        }
        cursor.remove();
    }

    function appendLine(who, text, isHtml = false, isTyping = false) {
        return new Promise(async (resolve) => {
            const line = document.createElement('div');
            line.className = 'terminal-line';

            const whoSpan = document.createElement('span');
            whoSpan.className = 'terminal-input';

            if (who === 'user') {
                whoSpan.style.color = '#fff';
            } else if (who === 'sys') {
                whoSpan.style.color = '#888';
            } else {
                whoSpan.style.color = '#0f0';
            }

            line.appendChild(whoSpan);
            terminalBody.appendChild(line);

            if (isTyping) {
                await typeText(line, text);
            } else if (isHtml) {
                const contentSpan = document.createElement('span');
                contentSpan.innerHTML = text;
                line.appendChild(contentSpan);
            } else {
                line.appendChild(document.createTextNode(text));
            }

            // Auto scroll down
            const mockupArea = document.querySelector('.terminal-mockup');
            mockupArea.scrollTop = mockupArea.scrollHeight;
            resolve();
        });
    }

    function parseMarkdown(text) {
        // Convert Markdown to HTML for Rich Media Responses
        let html = text;
        // Bold
        html = html.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
        // Inline Code
        html = html.replace(/`(.*?)`/g, '<code style="background: rgba(255,255,255,0.1); padding: 2px 4px; border-radius: 4px; color: #ffeb3b;">$1</code>');
        // Multiline Code Blocks
        html = html.replace(/```([\s\S]*?)```/g, '<pre style="background: rgba(0,0,0,0.5); padding: 10px; border-radius: 8px; overflow-x: auto; border: 1px solid var(--glass-border); color: #fff; margin: 10px 0;"><code>$1</code></pre>');
        // Lists
        html = html.replace(/^- (.*)$/gm, '<li style="margin-left: 20px;">$1</li>');
        return html;
    }

    if (terminalInputArea) {
        // Handle Multimodal File Visual Feedback
        const fileInput = document.getElementById('file-upload');
        const attachmentBtn = document.querySelector('.attachment-btn');
        let attachedFiles = [];

        if (fileInput && attachmentBtn) {
            fileInput.addEventListener('change', (e) => {
                const files = e.target.files;
                if (files.length > 0) {
                    attachedFiles = files;
                    attachmentBtn.style.color = '#00f0ff'; // Highlight cyan
                    attachmentBtn.style.opacity = '1';
                    // Optional: tooltip update
                    attachmentBtn.setAttribute('title', `${files.length} fayl biriktirildi.`);
                } else {
                    attachedFiles = [];
                    attachmentBtn.style.color = 'inherit';
                    attachmentBtn.style.opacity = '0.6';
                    attachmentBtn.setAttribute('title', 'Rasm yoki fayl biriktirish');
                }
            });
        }

        terminalInputArea.addEventListener('submit', async (e) => {
            e.preventDefault();
            const message = terminalInput.value.trim();
            if (!message && attachedFiles.length === 0) return;

            terminalInput.value = ''; // clear
            
            // 1. Show User Message (with attachment info)
            let userMsgDisplay = message;
            if (attachedFiles.length > 0) {
                userMsgDisplay += ` <span style="color:#00f0ff; font-size:0.8rem;">[📎 ${attachedFiles.length} file(s) attached]</span>`;
            }
            // Clear attachments visually after send
            if (fileInput) fileInput.value = '';
            attachedFiles = [];
            if (attachmentBtn) {
                attachmentBtn.style.color = 'inherit';
                attachmentBtn.style.opacity = '0.6';
            }

            await appendLine('user', userMsgDisplay, true, false);

            // 2. Show System thinking with Orchestration
            await appendLine('sys', '[ORCHESTRATOR] Connecting to Universal AI Mesh...', false, false);
            await sleep(400);
            await appendLine('sys', '[ANALYSIS] Orchestrating: GPT-5.2 | Gemini 3.1 | Grok-2 | Claude 3.5...', false, false);
            await sleep(600);
            await appendLine('sys', '[CONSENSUS] Cross-referencing results for ultimate precision...', false, false);
            await sleep(300);

            try {
                // Determine user language via browser navigator
                let userLang = 'en';
                if (navigator.language) {
                    const langMatch = navigator.language.split('-')[0].toLowerCase();
                    const supportedLangs = ['uz', 'ru', 'en', 'tr', 'es', 'zh', 'fr', 'de', 'ja', 'ar'];
                    if (supportedLangs.includes(langMatch)) {
                        userLang = langMatch;
                    }
                }

                // Contact Python Backend (Railway Server API)
                const res = await fetch(`/api/v1/ai/chat`, {
                    method: 'POST',
                    headers: {
                        'accept': 'application/json',
                        'Content-Type': 'application/json',
                        'Idempotency-Key': 'demo-' + Math.random().toString(36).substring(7),
                        'Authorization': 'Bearer mock-demo-token'
                    },
                    body: JSON.stringify({ message: message, model: "gpt-4o", language: userLang })
                });

                if (!res.ok) {
                    const errorData = await res.json().catch(() => ({}));
                    throw new Error(`Server returned ${res.status}: ${errorData.detail || errorData.message || 'Unknown'}`);
                }

                const data = await res.json();

                // Show Source
                await appendLine('sys', `[ROUTER] Routed to: ${data.source}`, false, false);

                // Render based on structured response type
                if (data.type === "video") {
                    const videoHtml = `
                       <div style="margin-top:10px; border:1px solid rgba(0, 240, 255, 0.3); padding:10px; border-radius:12px; background:rgba(0,0,0,0.6);">
                         <p style="color:#00f0ff; margin-bottom:10px; font-size: 0.9rem;">🎬 ${data.content}</p>
                         <video width="100%" controls style="border-radius:8px; display:block;">
                            <source src="${data.url}" type="video/mp4">
                            Sizning brauzeringiz videoni qo'llab-quvvatlamaydi.
                         </video>
                       </div>
                     `;
                    await appendLine('agent', videoHtml, true, false);
                } else if (data.type === "image") {
                    const imageHtml = `
                       <div style="margin-top:15px; border: 1px solid rgba(0, 240, 255, 0.3); padding:10px; border-radius:12px; background: rgba(0,0,0,0.4); text-align:center;">
                         <p style="color: #888; margin-bottom: 10px; font-size: 0.85rem;">${data.content}</p>
                         <img src="${data.url}" style="max-width:100%; border-radius:8px; box-shadow: 0 5px 15px rgba(0,0,0,0.5);"/>
                       </div>
                    `;
                    await appendLine('agent', imageHtml, true, false);
                } else {
                    // Type regular text response (Now supporting Rich Media Markdown)
                    const parsedContent = parseMarkdown(data.content);
                    await appendLine('agent', parsedContent, true, false); // true for HTML to inject nodes, false for typing to display parsed immediately 
                }
            } catch (err) {
                await appendLine('sys', `[ERROR] Connection failed: ${err.message}`, false, false);
            }
        });
    }

    // Connect Central Search to Terminal
    const mainSearchForm = document.getElementById('main-search-form');
    const mainSearchInput = document.getElementById('main-search-input');
    const termInput = document.getElementById('terminal-user-input');
    const termForm = document.getElementById('terminal-input-form');

    if (mainSearchForm && mainSearchInput && termInput && termForm) {
        mainSearchForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const val = mainSearchInput.value.trim();
            if (!val) return;
            
            // Sync value to terminal and scroll
            termInput.value = val;
            const demoSection = document.getElementById('demo');
            window.scrollTo({ top: demoSection.offsetTop - 80, behavior: 'smooth' });
            
            // Trigger terminal submit
            setTimeout(() => {
                termForm.dispatchEvent(new Event('submit'));
                mainSearchInput.value = ''; // clear main
            }, 600);
        });
    }

    // Initialize with a welcome message
    appendLine('sys', 'APEX AI Terminal v1.0 initialized.');

    // Smooth Scrolling for anchor links
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            e.preventDefault();
            const targetId = this.getAttribute('href');
            if (targetId === '#') return;
            const targetElement = document.querySelector(targetId);
            if (targetElement) {
                window.scrollTo({
                    top: targetElement.offsetTop - 80, // adjust for fixed navbar
                    behavior: 'smooth'
                });
            }
        });
    });

    // Pricing Toggle Logic
    const pricingToggle = document.getElementById('pricing-toggle');
    const priceVals = document.querySelectorAll('.price-val');

    if (pricingToggle) {
        pricingToggle.addEventListener('change', (e) => {
            const isYearly = e.target.checked;
            priceVals.forEach(val => {
                if (isYearly) {
                    val.innerText = val.getAttribute('data-yearly');
                } else {
                    val.innerText = val.getAttribute('data-monthly');
                }
            });
        });
    }

    // Stripe Checkout Integration
    const buyBtns = document.querySelectorAll('.buy-btn');
    buyBtns.forEach(btn => {
        btn.addEventListener('click', async (e) => {
            e.preventDefault();
            const tier = btn.getAttribute('data-tier');
            const cycle = pricingToggle && pricingToggle.checked ? 'yearly' : 'monthly';

            // Show Loading State
            const originalText = btn.innerText;
            btn.innerText = "Processing...";
            btn.style.opacity = "0.7";

            try {
                const res = await fetch(`/api/v1/payments/create-checkout-session`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        plan_tier: tier,
                        billing_cycle: cycle
                    })
                });

                if (!res.ok) throw new Error("Payment initialization failed");
                const data = await res.json();

                // Redirect user to Stripe Checkout URL (or simulated URL)
                if (data.session_url) {
                    // For dev simulator we redirect inside or mock
                    if (data.session_url.startsWith('/')) {
                        btn.innerText = "Current Plan";
                        alert("Standard Free Plan Activated Successfully!");
                    } else {
                        btn.innerText = "Redirecting...";
                        window.location.href = data.session_url;
                        setTimeout(() => { btn.innerText = originalText; btn.style.opacity = "1"; }, 3000);
                    }
                }
            } catch (err) {
                alert("Xato: To'lov tizimiga ulanib bo'lmadi. Python serveringiz yoniqligini tekshiring.");
                btn.innerText = originalText;
                btn.style.opacity = "1";
            }
        });
    });

    // Voice Visualizer Logic
    const voiceBtn = document.getElementById('voice-mode-btn');
    const visualizer = document.getElementById('voice-visualizer');

    if (voiceBtn && visualizer) {
        voiceBtn.addEventListener('click', () => {
            // Toggle Display
            if (visualizer.style.display === 'none') {
                visualizer.style.display = 'flex';
                voiceBtn.innerHTML = '<span class="pulse" style="background:#ff3333; box-shadow:0 0 8px #ff3333;"></span> Stop Listening';
            } else {
                visualizer.style.display = 'none';
                voiceBtn.innerHTML = '<span class="pulse" style="background:var(--accent-purple); box-shadow:0 0 8px var(--accent-purple);"></span> Try Voice AI';
            }
        });
    }

    // Scroll Fade-in Animation Logic
    const fadeElements = document.querySelectorAll('.fade-in');
    if (fadeElements.length > 0) {
        const appearOptions = {
            threshold: 0.15,
            rootMargin: "0px 0px -50px 0px"
        };
        const appearOnScroll = new IntersectionObserver(function (entries, observer) {
            entries.forEach(entry => {
                if (!entry.isIntersecting) return;
                entry.target.classList.add('visible');
                observer.unobserve(entry.target);
            });
        }, appearOptions);

        fadeElements.forEach(element => {
            appearOnScroll.observe(element);
        });
    }

    // --- Auth Modal Logic ---
    const authOpenBtn = document.getElementById('btn-auth-open');
    const authModal = document.getElementById('auth-modal');
    const authCloseBtn = document.getElementById('auth-close');
    const authTabs = document.querySelectorAll('.auth-tab');
    const authForms = document.querySelectorAll('.auth-form');

    if (authOpenBtn && authModal) {
        authOpenBtn.addEventListener('click', (e) => {
            e.preventDefault();
            authModal.classList.add('active');
        });
    }

    if (authCloseBtn) {
        authCloseBtn.addEventListener('click', () => {
            authModal.classList.remove('active');
        });
    }

    // Close on clicking outside
    window.addEventListener('click', (e) => {
        if (e.target === authModal) {
            authModal.classList.remove('active');
        }
    });

    // Tab switching
    authTabs.forEach(tab => {
        tab.addEventListener('click', () => {
            // Remove active from all tabs
            authTabs.forEach(t => t.classList.remove('active'));
            // Remove active from all forms
            authForms.forEach(f => f.classList.remove('active'));

            // Set current active
            tab.classList.add('active');
            const targetId = tab.getAttribute('data-target');
            document.getElementById(targetId).classList.add('active');
        });
    });

});
