document.addEventListener('DOMContentLoaded', () => {

    const refreshBtn = document.getElementById('refresh-db-btn');
    const tableBody = document.getElementById('users-table-body');
    const searchInput = document.querySelector('.search-input');

    // MOCK DATA FOR DEMONSTRATION (Until API is wired directly)
    const MOCK_DB = [
        { id: '#8819', name: 'Alisher Rustamov', email: 'alisher.r@gmail.com', tier: 'pro', date: '24.03.2026', status: 'Faol' },
        { id: '#8820', name: 'Gulnoza Karimova', email: 'g.karim22@mail.ru', tier: 'free', date: '24.03.2026', status: 'Faol' },
        { id: '#8821', name: 'Murod Jo\'rayev', email: 'm.jorayev_corp@yandex.com', tier: 'premium', date: '23.03.2026', status: 'Faol' },
        { id: '#8822', name: 'Zarina Tohirova', email: 'zt2000@gmail.com', tier: 'plus', date: '22.03.2026', status: 'Faol' },
        { id: '#8823', name: 'Oybek AI Agency', email: 'contact@oybek-ai.uz', tier: 'premium', date: '22.03.2026', status: 'Bloklangan' }
    ];

    function renderTable(data) {
        tableBody.innerHTML = '';
        if (data.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="7" style="text-align:center; padding: 20px;">Ma\'lumot topilmadi</td></tr>';
            return;
        }

        data.forEach(user => {
            // Determine badge style
            let badgeHtml = '';
            if (user.tier === 'free') badgeHtml = '<span class="badge badge-free">Free ($0)</span>';
            else if (user.tier === 'plus') badgeHtml = '<span class="badge badge-pro" style="color:white; border-color:white;">Plus ($3)</span>';
            else if (user.tier === 'pro') badgeHtml = '<span class="badge badge-pro">Pro ($9)</span>';
            else if (user.tier === 'premium') badgeHtml = '<span class="badge badge-premium">Premium ($19)</span>';

            // Determine status dot
            let statusHtml = '<span class="status-dot green"></span> Faol';
            if (user.status !== 'Faol') statusHtml = '<span class="status-dot" style="background:#ff4d4d; box-shadow:0 0 8px #ff4d4d;"></span> ' + user.status;

            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${user.id}</td>
                <td><strong>${user.name}</strong></td>
                <td style="color: var(--text-second);">${user.email}</td>
                <td>${badgeHtml}</td>
                <td>${user.date}</td>
                <td>${statusHtml}</td>
                <td><button class="action-btn">Tahrir</button></td>
            `;
            tableBody.appendChild(tr);
        });
    }

    // Initial Load
    renderTable(MOCK_DB);

    // Refresh Action (Simulates API Hit)
    refreshBtn.addEventListener('click', () => {
        refreshBtn.innerText = 'Yuklanmoqda...';
        refreshBtn.style.opacity = '0.7';
        
        // Simulate network delay
        setTimeout(() => {
            // Add a new random user to simulate DB update
            const newId = '#88' + Math.floor(Math.random() * 90 + 24);
            MOCK_DB.unshift({
                id: newId, 
                name: 'Yangi Mijoz', 
                email: 'client' + newId.slice(1) + '@mail.com', 
                tier: 'free', 
                date: 'Hozirgina', 
                status: 'Faol'
            });
            renderTable(MOCK_DB);
            
            refreshBtn.innerText = '🔄 Bazani Yangilash';
            refreshBtn.style.opacity = '1';
        }, 800);
    });

    // Search Logic
    searchInput.addEventListener('input', (e) => {
        const query = e.target.value.toLowerCase();
        const filtered = MOCK_DB.filter(u => 
            u.name.toLowerCase().includes(query) || 
            u.email.toLowerCase().includes(query)
        );
        renderTable(filtered);
    });

});
