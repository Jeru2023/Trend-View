export const financialDataTab = {
  id: "financialData",
  template: "tabs/financial-data.html",
  columnCount: 10,
  dataSource: "trading",
  render(items, ctx) {
    const { body } = ctx;
    if (!body) {
      return;
    }

    body.innerHTML = "";

    if (!items.length) {
      ctx.renderEmptyRow(body, financialDataTab.columnCount);
      return;
    }

    items.forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${item.code}</td>
        <td>${item.name ?? ctx.emptyValue}</td>
        <td>${ctx.formatOptionalDate(item.ann_date)}</td>
        <td>${ctx.formatOptionalDate(item.end_date)}</td>
        <td>${ctx.formatOptionalNumber(item.basic_eps, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })}</td>
        <td>${ctx.formatOptionalNumber(
          item.revenue === null || item.revenue === undefined ? null : item.revenue / 1_000_000,
          { maximumFractionDigits: 2 }
        )}</td>
        <td>${ctx.formatOptionalNumber(
          item.operate_profit === null || item.operate_profit === undefined
            ? null
            : item.operate_profit / 1_000_000,
          { maximumFractionDigits: 2 }
        )}</td>
        <td>${ctx.formatOptionalNumber(
          item.net_income === null || item.net_income === undefined ? null : item.net_income / 1_000_000,
          { maximumFractionDigits: 2 }
        )}</td>
        <td>${ctx.formatOptionalNumber(
          item.gross_margin === null || item.gross_margin === undefined
            ? null
            : item.gross_margin / 1_000_000,
          { maximumFractionDigits: 2 }
        )}</td>
        <td>${ctx.formatFinancialPercent(item.roe)}</td>
      `;
      body.appendChild(row);
    });
  },
};
