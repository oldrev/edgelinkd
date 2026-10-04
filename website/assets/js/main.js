/* EdgeLinkd website — progressive enhancement.
 * Theme (light / dark / system), language preference, mobile menu, language
 * dropdown, copy buttons, header state and scroll spy. No dependencies.
 */
(function () {
  "use strict";

  var root = document.documentElement;
  var THEME_KEY = "edgelinkd:theme";
  var LANG_KEY = "edgelinkd:lang";
  var media = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;

  root.classList.add("js-theme");

  function save(key, value) {
    try { localStorage.setItem(key, value); } catch (e) { /* storage disabled */ }
  }

  function load(key) {
    try { return localStorage.getItem(key); } catch (e) { return null; }
  }

  /* ---------- theme ---------- */

  function themePref() {
    var v = load(THEME_KEY);
    return v === "light" || v === "dark" ? v : "system";
  }

  function applyTheme(pref) {
    var dark = pref === "dark" || (pref === "system" && !!media && media.matches);
    root.setAttribute("data-theme", dark ? "dark" : "light");
    root.setAttribute("data-theme-pref", pref);
    root.style.colorScheme = dark ? "dark" : "light";
    var buttons = document.querySelectorAll("[data-theme-value]");
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].setAttribute("aria-checked", buttons[i].getAttribute("data-theme-value") === pref ? "true" : "false");
    }
  }

  applyTheme(themePref());

  if (media) {
    var onSchemeChange = function () {
      if (themePref() === "system") applyTheme("system");
    };
    if (media.addEventListener) media.addEventListener("change", onSchemeChange);
    else if (media.addListener) media.addListener(onSchemeChange);
  }

  /* ---------- mobile menu ---------- */

  var menu = document.getElementById("mobile-menu");
  var menuBtn = document.querySelector("[data-menu-toggle]");

  function setMenu(open) {
    if (!menu || !menuBtn) return;
    if (open) menu.removeAttribute("hidden");
    else menu.setAttribute("hidden", "");
    menuBtn.setAttribute("aria-expanded", open ? "true" : "false");
  }

  function closeDropdowns(except) {
    var open = document.querySelectorAll("details[data-dropdown][open]");
    for (var i = 0; i < open.length; i++) {
      if (!except || !open[i].contains(except)) open[i].removeAttribute("open");
    }
  }

  /* ---------- copy to clipboard ---------- */

  function codeText(pre) {
    var lines = pre.querySelectorAll(".line");
    if (!lines.length) return pre.textContent.replace(/\n$/, "");
    var out = [];
    for (var i = 0; i < lines.length; i++) {
      if (!lines[i].classList.contains("is-comment")) out.push(lines[i].textContent);
    }
    return out.join("\n");
  }

  function copied(btn) {
    btn.classList.add("is-copied");
    setTimeout(function () { btn.classList.remove("is-copied"); }, 1600);
  }

  function copy(text, btn) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(function () { copied(btn); }, function () {});
      return;
    }
    var ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand("copy"); copied(btn); } catch (e) { /* ignore */ }
    document.body.removeChild(ta);
  }

  // Markdown code blocks get a copy button cloned from <template id="tpl-copy-btn">.
  var tpl = document.getElementById("tpl-copy-btn");
  if (tpl && "content" in tpl) {
    var blocks = document.querySelectorAll(".prose pre");
    for (var b = 0; b < blocks.length; b++) {
      blocks[b].appendChild(tpl.content.firstElementChild.cloneNode(true));
    }
  }

  /* ---------- delegated clicks ---------- */

  document.addEventListener("click", function (e) {
    var t = e.target;
    if (!t || !t.closest) return;

    closeDropdowns(t);

    var themeBtn = t.closest("[data-theme-value]");
    if (themeBtn) {
      var pref = themeBtn.getAttribute("data-theme-value");
      save(THEME_KEY, pref);
      applyTheme(pref);
      return;
    }

    var langLink = t.closest("a[data-lang]");
    if (langLink) {
      save(LANG_KEY, langLink.getAttribute("data-lang"));
      return;
    }

    var copyBtn = t.closest("[data-copy]");
    if (copyBtn) {
      var host = copyBtn.closest(".codeblock") || copyBtn.parentElement;
      var pre = host && (host.tagName === "PRE" ? host : host.querySelector("pre"));
      if (pre) copy(codeText(pre), copyBtn);
      return;
    }

    if (t.closest("[data-menu-toggle]")) {
      setMenu(menu ? menu.hasAttribute("hidden") : false);
      return;
    }

    if (menu && t.closest("#mobile-menu a")) setMenu(false);
  });

  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape") {
      closeDropdowns(null);
      setMenu(false);
    }
  });

  window.addEventListener("resize", function () {
    if (window.innerWidth > 960) setMenu(false);
  });

  /* ---------- header state ---------- */

  var header = document.querySelector("[data-header]");
  function onScroll() {
    if (header) header.classList.toggle("is-scrolled", window.scrollY > 8);
  }
  onScroll();
  window.addEventListener("scroll", onScroll, { passive: true });

  /* ---------- scroll spy (home page) ---------- */

  if ("IntersectionObserver" in window && document.body.classList.contains("is-home")) {
    var links = document.querySelectorAll('.nav a[href*="#"]');
    var byId = {};
    for (var l = 0; l < links.length; l++) {
      var id = links[l].getAttribute("href").split("#")[1];
      var section = id && document.getElementById(id);
      if (section) byId[id] = links[l];
    }
    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (!entry.isIntersecting) return;
        for (var key in byId) byId[key].classList.toggle("is-active", key === entry.target.id);
      });
    }, { rootMargin: "-45% 0px -50% 0px" });
    for (var key in byId) observer.observe(document.getElementById(key));
  }
})();
