(() => {
    const cfg = window.ONDemandConfig || {};
    const MAX_DEPTH = Number(cfg.maxDepth || 5);
    const UPLOAD_ALLOWED_DEPTH = Number(cfg.uploadAllowedDepth || 5);

    const folderTree = document.getElementById("folderTree");
    const currentPath = document.getElementById("currentPath");
    const currentMeta = document.getElementById("currentMeta");
    const fileTableBody = document.getElementById("fileTableBody");
    const dropZone = document.getElementById("dropZone");
    const dropZoneSub = document.getElementById("dropZoneSub");
    const logBox = document.getElementById("logBox");
    const refreshBtn = document.getElementById("refreshBtn");
    const fileInput = document.getElementById("fileInput");
    const fileSelectBtn = document.getElementById("fileSelectBtn");
    const listEmpty = document.getElementById("listEmpty");
    const listTableWrap = document.getElementById("listTableWrap");
    const listState = document.getElementById("listState");

    let selectedPath = "";
    let selectedDepth = 0;
    let selectedCanUpload = false;

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function formatBytes(bytes) {
        const n = Number(bytes || 0);
        if (!n) return "-";
        const units = ["B", "KB", "MB", "GB", "TB"];
        let i = 0;
        let v = n;
        while (v >= 1024 && i < units.length - 1) {
            v /= 1024;
            i += 1;
        }
        return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
    }

    function addLog(type, text) {
        const row = document.createElement("div");
        row.className = `ondemandLogRow ${type || ""}`;
        row.textContent = text;
        logBox.prepend(row);
    }

    async function fetchJson(url, options = {}) {
        const res = await fetch(url, options);
        const data = await res.json().catch(() => ({}));
        if (!res.ok || data.ok === false) {
            throw new Error(data.error || `HTTP ${res.status}`);
        }
        return data;
    }

    function setUploadState(canUpload, depth) {
        selectedCanUpload = !!canUpload;
        selectedDepth = Number(depth || 0);

        if (selectedCanUpload) {
            dropZone.classList.remove("disabled");
            fileSelectBtn.disabled = false;
            dropZoneSub.textContent = `Lv${UPLOAD_ALLOWED_DEPTH}フォルダです。ファイル追加できます。`;
        } else {
            dropZone.classList.add("disabled");
            fileSelectBtn.disabled = true;
            dropZoneSub.textContent = `Lv${UPLOAD_ALLOWED_DEPTH}フォルダ選択時のみファイル追加できます（現在: Lv${selectedDepth}）`;
        }
    }

    function setListVisible(visible) {
        if (visible) {
            listEmpty.classList.add("hidden");
            listTableWrap.classList.remove("hidden");
            listState.textContent = "表示中";
        } else {
            listTableWrap.classList.add("hidden");
            listEmpty.classList.remove("hidden");
            fileTableBody.innerHTML = "";
            listState.textContent = "未選択";
        }
    }

    function renderTable(dirs, files) {
        fileTableBody.innerHTML = "";

        const hasDirs = Array.isArray(dirs) && dirs.length > 0;
        const hasFiles = Array.isArray(files) && files.length > 0;

        if (!hasDirs && !hasFiles) {
            const tr = document.createElement("tr");
            tr.innerHTML = `<td colspan="4" class="empty-cell">ファイルはありません。</td>`;
            fileTableBody.appendChild(tr);
            return;
        }

        for (const d of dirs || []) {
            const tr = document.createElement("tr");
            tr.className = "clickable-row";
            tr.innerHTML = `
        <td>📁</td>
        <td>${escapeHtml(d.name)}</td>
        <td>${escapeHtml(d.mtime || "-")}</td>
        <td>-</td>
      `;
            tr.addEventListener("click", () => loadFolder(d.path || ""));
            fileTableBody.appendChild(tr);
        }

        for (const f of files || []) {
            const tr = document.createElement("tr");
            tr.innerHTML = `
        <td>📄</td>
        <td>${escapeHtml(f.name)}</td>
        <td>${escapeHtml(f.mtime || "-")}</td>
        <td>${escapeHtml(formatBytes(f.size_bytes))}</td>
      `;
            fileTableBody.appendChild(tr);
        }
    }

    function matchesLevelRule(item) {
        const depth = Number(item?.depth || 0);
        const name = String(item?.name || "");

        if (depth === 1) {
            return [...name].length === 1;
        }
        if (depth === 2) {
            return [...name].length === 2;
        }
        if (depth === 4) {
            return name === "元データ";
        }
        return true;
    }

    function filterDirsByRule(dirs) {
        return (dirs || []).filter(matchesLevelRule);
    }

    function closeSiblingBranches(currentWrapper) {
        const parentChildren = currentWrapper.parentElement;
        if (!parentChildren) return;

        const siblings = parentChildren.querySelectorAll(":scope > .folderTreeItem.expanded");
        siblings.forEach((sib) => {
            if (sib === currentWrapper) return;
            sib.classList.remove("expanded");

            const toggle = sib.querySelector(":scope > .folderTreeRow > .folderTreeToggle");
            const children = sib.querySelector(":scope > .folderTreeChildren");

            if (toggle && !toggle.disabled) {
                toggle.textContent = "+";
            }
            if (children) {
                children.hidden = true;
            }
        });
    }

    function closeAllOtherBranches(currentWrapper) {
        const expanded = folderTree.querySelectorAll(".folderTreeItem.expanded");
        expanded.forEach((item) => {
            if (item === currentWrapper) return;
            if (item.contains(currentWrapper)) return;
            if (currentWrapper.contains(item)) return;

            item.classList.remove("expanded");
            const toggle = item.querySelector(":scope > .folderTreeRow > .folderTreeToggle");
            const children = item.querySelector(":scope > .folderTreeChildren");
            if (toggle && !toggle.disabled) {
                toggle.textContent = "+";
            }
            if (children) {
                children.hidden = true;
            }
        });
    }

    function makeTreeNode(item) {
        const wrapper = document.createElement("div");
        wrapper.className = "folderTreeItem";
        wrapper.dataset.path = item.path || "";
        wrapper.dataset.depth = String(item.depth || 0);

        const row = document.createElement("div");
        row.className = "folderTreeRow";

        const toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = "folderTreeToggle";
        toggle.textContent = item.depth < MAX_DEPTH && item.has_children ? "+" : "";
        toggle.disabled = !(item.depth < MAX_DEPTH && item.has_children);

        const nodeBtn = document.createElement("button");
        nodeBtn.type = "button";
        nodeBtn.className = "folderTreeNode";
        nodeBtn.innerHTML = `
      <span class="folderTreeName">${escapeHtml(item.name || "/")}</span>
      <span class="folderTreeLv">Lv${escapeHtml(item.depth)}</span>
    `;

        const children = document.createElement("div");
        children.className = "folderTreeChildren";
        children.hidden = true;

        toggle.addEventListener("click", async (e) => {
            e.stopPropagation();
            if (toggle.disabled) return;

            const expanded = wrapper.classList.contains("expanded");
            if (expanded) {
                wrapper.classList.remove("expanded");
                children.hidden = true;
                toggle.textContent = "+";
                return;
            }

            try {
                closeSiblingBranches(wrapper);
                closeAllOtherBranches(wrapper);

                if (!children.dataset.loaded) {
                    const data = await fetchJson(`/api/explorer/list?path=${encodeURIComponent(item.path || "")}`);
                    const filteredDirs = filterDirsByRule(data.dirs || []);

                    children.innerHTML = "";
                    for (const child of filteredDirs) {
                        if (Number(child.depth) <= MAX_DEPTH) {
                            children.appendChild(makeTreeNode(child));
                        }
                    }
                    children.dataset.loaded = "1";
                }

                wrapper.classList.add("expanded");
                children.hidden = false;
                toggle.textContent = "-";
            } catch (err) {
                addLog("err", `ツリー展開失敗: ${err.message}`);
            }
        });

        nodeBtn.addEventListener("click", async () => {
            await loadFolder(item.path || "");
            highlightSelectedTree(item.path || "");
        });

        row.appendChild(toggle);
        row.appendChild(nodeBtn);
        wrapper.appendChild(row);
        wrapper.appendChild(children);

        return wrapper;
    }

    function highlightSelectedTree(path) {
        document.querySelectorAll(".folderTreeItem.selected").forEach(el => el.classList.remove("selected"));
        const target = document.querySelector(`.folderTreeItem[data-path="${CSS.escape(path || "")}"]`);
        if (target) {
            target.classList.add("selected");
        }
    }

    async function loadTreeRoot() {
        const data = await fetchJson("/api/explorer/root");
        folderTree.innerHTML = "";
        folderTree.appendChild(makeTreeNode(data.root));
    }

    async function loadFolder(path) {
        try {
            const data = await fetchJson(`/api/explorer/list?path=${encodeURIComponent(path || "")}`);
            selectedPath = data.current?.path || "";
            currentPath.textContent = data.current?.abs_path || "-";
            currentMeta.textContent = `現在階層: Lv${data.current?.depth ?? 0} / 追加: ${data.current?.can_upload ? "可" : "不可"}`;
            setUploadState(!!data.current?.can_upload, Number(data.current?.depth || 0));

            const shouldShowList = !!data.current?.can_upload;
            setListVisible(shouldShowList);

            if (shouldShowList) {
                renderTable(data.dirs, data.files);
            }

            addLog("info", `フォルダ表示: ${data.current?.abs_path || "-"}`);
        } catch (err) {
            addLog("err", `読込失敗: ${err.message}`);
        }
    }

    async function uploadFiles(files) {
        if (!files || files.length === 0) return;

        if (!selectedCanUpload) {
            addLog("err", `Lv${UPLOAD_ALLOWED_DEPTH}のフォルダでのみアップロードできます。`);
            return;
        }

        const fd = new FormData();
        fd.append("path", selectedPath);
        for (const file of files) {
            fd.append("files", file, file.name);
        }

        try {
            addLog("info", `アップロード開始: ${files.length}件`);
            const data = await fetchJson("/api/explorer/upload", {
                method: "POST",
                body: fd
            });

            for (const s of data.saved || []) {
                addLog("ok", `保存: ${s.name}`);
            }
            for (const e of data.errors || []) {
                addLog("err", `失敗: ${e.name} / ${e.error}`);
            }
            for (const s of data.skipped || []) {
                addLog("warn", `スキップ: ${s.name} / ${s.reason}`);
            }

            await loadFolder(selectedPath);
        } catch (err) {
            addLog("err", `アップロード失敗: ${err.message}`);
        }
    }

    dropZone.addEventListener("dragover", (e) => {
        if (!selectedCanUpload) return;
        e.preventDefault();
        dropZone.classList.add("dragover");
    });

    dropZone.addEventListener("dragleave", () => {
        dropZone.classList.remove("dragover");
    });

    dropZone.addEventListener("drop", async (e) => {
        if (!selectedCanUpload) return;
        e.preventDefault();
        dropZone.classList.remove("dragover");
        const files = Array.from(e.dataTransfer.files || []);
        await uploadFiles(files);
    });

    fileSelectBtn.addEventListener("click", () => {
        if (!selectedCanUpload) return;
        fileInput.click();
    });

    fileInput.addEventListener("change", async (e) => {
        const files = Array.from(e.target.files || []);
        await uploadFiles(files);
        fileInput.value = "";
    });

    refreshBtn.addEventListener("click", async () => {
        await loadFolder(selectedPath || "");
    });

    (async () => {
        try {
            await loadTreeRoot();
            await loadFolder("");
            highlightSelectedTree("");
            addLog("info", `初期化完了: 最大Lv${MAX_DEPTH} / 追加可能Lv${UPLOAD_ALLOWED_DEPTH}`);
            addLog("info", "フィルタ: Lv1=1文字, Lv2=2文字, Lv4=元データ");
        } catch (err) {
            addLog("err", `初期化失敗: ${err.message}`);
        }
    })();
})();