from pathlib import Path

p = Path('/home/michael/dashboard/static/index.html')
html = p.read_text()

before = '    </div>\n\n  </div>\n</main>\n<div class="statusbar">'

# Check if our statusbar already sits right after </main>
if '</main>' in html and '</html>' in html:
    # remove stray duplicate planner blocks and extra scripts
    import re
    html = re.sub(r'\n\s*</main>\s*\n\s*<div class="statusbar">.*$', '', html, flags=re.S)
    html = re.sub(r'(\n  </main>)', r'\1<div class="statusbar">\n  <div class="statusbar-left">\n    <div class="sb-item" id="sbTime">Time: --</div>\n  </div>\n  <div class="statusbar-right">\n    <div class="sb-item" id="sbUptime">Uptime: loading</div>\n    <div class="sb-item sb-online" id="sbGateway"><div class="sb-dot" style="background:var(--green)"></div>Gateway online</div>\n  </div>\n</div>', html)
    html = re.sub(r'\s*<script>\nlet knownNotes = new Set\(\);\nlet knownQuizzes = new Set\(\);\nlet currentPage = \'overview\';\n.*?</script>\s*$', '', html, flags=re.S)

    # normalize duplicate planner + broken tail
    start = '<div id="page-planner" class="page">'
    first = html.find(start)
    second = html.find(start, first + 1)
    if second != -1:
        html = html[:second] + '</div>\n</body>\n</html>'
        html = re.sub(r'\s*<div class="notif-title" id="notifTitle"></div>\s*<div class="notif-body" id="notifBody"></div>\s*</div>', '', html)
        html = re.sub(r'\s*</body>\s*</html>\s*<script>.*</script>\s*$', '', html, flags=re.S)

    p.write_text(html)
    print('normalized')
else:
    print('missing main/html')
