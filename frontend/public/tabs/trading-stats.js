const STAT_FIELDS = [
  { key: "pct_change_1y", fromRatio: true },
  { key: "pct_change_6m", fromRatio: true },
  { key: "pct_change_3m", fromRatio: true },
  { key: "pct_change_1m", fromRatio: true },
  { key: "pct_change_2w", fromRatio: true },
  { key: "pct_change_1w", fromRatio: true },
  {
    key: "volume_spike",
    options: { minimumFractionDigits: 2, maximumFractionDigits: 2 },
    trend: "volume",
  },
  {
    key: "ma_20",
    options: { minimumFractionDigits: 2, maximumFractionDigits: 2 },
  },
  {
    key: "ma_10",
    options: { minimumFractionDigits: 2, maximumFractionDigits: 2 },
  },
  {
    key: "ma_5",
    options: { minimumFractionDigits: 2, maximumFractionDigits: 2 },
  },
];

export const tradingStatsTab = {
  id: "tradingStats",
  template: "tabs/trading-stats.html",
  columnCount: 12,
  dataSource: "trading",
  render(items, ctx) {
    const { body } = ctx;
    if (!body) {
      return;
    }

    body.innerHTML = "";

    if (!items.length) {
      ctx.renderEmptyRow(body, tradingStatsTab.columnCount);
      return;
    }

    const getDetailUrl =
      typeof ctx.buildDetailUrl === "function"
        ? ctx.buildDetailUrl
        : (code) => `stock-detail.html?code=${encodeURIComponent(code)}`;

    items.forEach((item) => {
      const row = document.createElement("tr");
      const detailUrl = getDetailUrl(item.code);
      const cells = [
        `<td><a class="table-link" href="${detailUrl}">${item.code}</a></td>`,
        `<td>${item.name ? `<a class="table-link" href="${detailUrl}">${item.name}</a>` : ctx.emptyValue}</td>`,
      ];

      STAT_FIELDS.forEach((field) => {
        const value = item[field.key];
        if (field.fromRatio) {
          const cellClass = ctx.getTrendClass(value);
          cells.push(`<td class="${cellClass}">${ctx.formatPercent(value, { fromRatio: true })}</td>`);
        } else {
          const options =
            field.options || {
              minimumFractionDigits: 2,
              maximumFractionDigits: 2,
            };
          let cellClass = "";
          if (field.trend === "volume") {
            const baseline = value === null || value === undefined ? null : value - 1;
            cellClass = ctx.getTrendClass(baseline);
          }
          const formatted = ctx.formatOptionalNumber(value, options);
          cells.push(`<td${cellClass ? ` class="${cellClass}"` : ""}>${formatted}</td>`);
        }
      });

      row.innerHTML = cells.join("");
      body.appendChild(row);
    });
  },
};
