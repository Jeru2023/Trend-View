const STAT_FIELDS = [
  { key: "pct_change_1y", fromRatio: true },
  { key: "pct_change_6m", fromRatio: true },
  { key: "pct_change_3m", fromRatio: true },
  { key: "pct_change_1m", fromRatio: true },
  { key: "pct_change_2w", fromRatio: true },
  { key: "pct_change_1w", fromRatio: true },
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
  columnCount: 11,
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

    items.forEach((item) => {
      const row = document.createElement("tr");
      const cells = [
        `<td>${item.code}</td>`,
        `<td>${item.name ?? ctx.emptyValue}</td>`,
      ];

      STAT_FIELDS.forEach((field) => {
        if (field.fromRatio) {
          const value = item[field.key];
          const cellClass = ctx.getTrendClass(value);
          cells.push(`<td class="${cellClass}">${ctx.formatPercent(value, { fromRatio: true })}</td>`);
        } else {
          cells.push(
            `<td>${ctx.formatOptionalNumber(item[field.key], field.options || {
              minimumFractionDigits: 2,
              maximumFractionDigits: 2,
            })}</td>`
          );
        }
      });

      row.innerHTML = cells.join("");
      body.appendChild(row);
    });
  },
};
