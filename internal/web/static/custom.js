// CIO Browser JavaScript

// Theme toggle
document.addEventListener('DOMContentLoaded', function() {
    const themeToggle = document.getElementById('theme-toggle');
    const html = document.documentElement;

    // Load saved theme preference
    const savedTheme = localStorage.getItem('theme') || 'light';
    html.classList.remove('light', 'dark');
    html.classList.add(savedTheme);

    if (themeToggle) {
        themeToggle.addEventListener('click', function() {
            const currentTheme = html.classList.contains('dark') ? 'dark' : 'light';
            const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

            html.classList.remove('light', 'dark');
            html.classList.add(newTheme);
            localStorage.setItem('theme', newTheme);
        });
    }

    // View toggle (grid/list)
    const viewToggle = document.getElementById('view-toggle');
    const body = document.body;

    // Load saved view preference
    const savedView = localStorage.getItem('view') || 'grid';
    body.classList.remove('view-grid', 'view-list');
    body.classList.add('view-' + savedView);

    if (viewToggle) {
        viewToggle.addEventListener('click', function() {
            const currentView = body.classList.contains('view-list') ? 'list' : 'grid';
            const newView = currentView === 'list' ? 'grid' : 'list';

            body.classList.remove('view-grid', 'view-list');
            body.classList.add('view-' + newView);
            localStorage.setItem('view', newView);
        });
    }

    // Update current path when browsing
    document.body.addEventListener('htmx:afterSwap', function(event) {
        if (event.detail.target.id === 'resource-list') {
            // Extract path from the request URL
            const url = new URL(event.detail.xhr.responseURL);
            const path = url.searchParams.get('path');
            if (path) {
                document.getElementById('currentPath').value = path;
            }
        }
    });

    // Highlight active alias
    document.body.addEventListener('click', function(event) {
        const aliasBtn = event.target.closest('.alias-btn');
        if (aliasBtn) {
            // Remove active class from all alias buttons
            document.querySelectorAll('.alias-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            // Add active class to clicked button
            aliasBtn.classList.add('active');
        }
    });
});

// File preview function
function previewFile(path, name) {
    const modal = document.getElementById('preview-modal');
    const title = document.getElementById('preview-title');
    const content = document.getElementById('preview-content');

    // Show modal
    modal.classList.remove('hidden');
    title.textContent = name;
    content.innerHTML = '<div class="text-center py-8"><svg class="animate-spin h-8 w-8 mx-auto text-gray-400" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg><p class="mt-2 text-gray-500">Loading preview...</p></div>';

    // Fetch preview
    fetch('/api/preview?path=' + encodeURIComponent(path))
        .then(response => response.json())
        .then(data => {
            if (data.tooLarge) {
                content.innerHTML = `
                    <div class="text-center py-8">
                        <svg class="h-16 w-16 mx-auto text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/>
                        </svg>
                        <p class="mt-4 text-gray-600 dark:text-gray-400">${data.error}</p>
                        <p class="mt-2 text-sm text-gray-500 dark:text-gray-500">File size: ${formatBytes(data.size)}</p>
                    </div>
                `;
            } else if (data.error) {
                content.innerHTML = `
                    <div class="text-center py-8">
                        <svg class="h-16 w-16 mx-auto text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
                        </svg>
                        <p class="mt-4 text-red-600 dark:text-red-400">${data.error}</p>
                    </div>
                `;
            } else {
                // Display content based on mime type
                if (data.mimeType && data.mimeType.startsWith('image/')) {
                    content.innerHTML = `<img src="data:${data.mimeType};base64,${btoa(data.content)}" class="max-w-full h-auto mx-auto" alt="${name}">`;
                } else {
                    content.innerHTML = `<pre class="text-sm overflow-x-auto">${escapeHtml(data.content)}</pre>`;
                }
            }
        })
        .catch(error => {
            content.innerHTML = `
                <div class="text-center py-8">
                    <svg class="h-16 w-16 mx-auto text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
                    </svg>
                    <p class="mt-4 text-red-600 dark:text-red-400">Failed to load preview</p>
                    <p class="mt-2 text-sm text-gray-500">${error.message}</p>
                </div>
            `;
        });
}

// Helper function to escape HTML
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Helper function to format bytes
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

// Close modal when clicking outside
document.addEventListener('click', function(event) {
    const modal = document.getElementById('preview-modal');
    if (event.target === modal) {
        modal.classList.add('hidden');
    }
});

// Close modal with Escape key
document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape') {
        const modal = document.getElementById('preview-modal');
        modal.classList.add('hidden');
    }
});
