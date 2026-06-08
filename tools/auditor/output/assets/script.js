function selectExchange(btn) {
  var section = btn.closest('section.route');
  var ex = btn.dataset.exchange;
  section.dataset.activeExchange = ex;
  section.querySelectorAll('.exchange-btn').forEach(function(b){ b.classList.toggle('active', b.dataset.exchange === ex); });
  applyPanelDisplay(section);
}

function selectVariant(btn) {
  var section = btn.closest('section.route');
  var vid = btn.dataset.variant;
  section.dataset.activeVariant = vid;
  section.querySelectorAll('.variant-btn').forEach(function(b){ b.classList.toggle('active', b.dataset.variant === vid); });
  section.querySelectorAll('.variant-block').forEach(function(blk){
    blk.style.display = (blk.dataset.variant === vid) ? 'block' : 'none';
  });
  applyPanelDisplay(section);
}

function selectView(btn) {
  var panel = btn.closest('.panel');
  var view = btn.dataset.view;
  panel.querySelectorAll('.view-btn').forEach(function(b){ b.classList.toggle('active', b.dataset.view === view); });
  panel.querySelectorAll('.panel-view').forEach(function(v){ v.classList.toggle('active', v.dataset.view === view); });
}

function applyPanelDisplay(section) {
  var vid = section.dataset.activeVariant;
  var ex = section.dataset.activeExchange;
  section.querySelectorAll('.panel').forEach(function(p){
    p.style.display = (p.dataset.variant === vid && p.dataset.exchange === ex) ? 'block' : 'none';
  });
}

function initAllSections() {
  document.querySelectorAll('section.route').forEach(function(section){
    var vid = section.dataset.activeVariant || 'default';
    section.querySelectorAll('.variant-block').forEach(function(blk){
      blk.style.display = (blk.dataset.variant === vid) ? 'block' : 'none';
    });
    applyPanelDisplay(section);
  });
}

function applyFilter() {
  var q = document.getElementById('search').value.trim().toLowerCase();
  var onlyIssues = document.getElementById('only-issues').checked;
  document.querySelectorAll('section.route').forEach(function(s){
    var name = (s.dataset.route || '').toLowerCase();
    var hasIssue = s.dataset.hasIssue === '1';
    var show = true;
    if (q && !name.includes(q)) show = false;
    if (onlyIssues && !hasIssue) show = false;
    s.classList.toggle('hidden', !show);
  });
}

document.addEventListener('DOMContentLoaded', function(){
  initAllSections();
  document.getElementById('search').addEventListener('input', applyFilter);
  document.getElementById('only-issues').addEventListener('change', applyFilter);
});
