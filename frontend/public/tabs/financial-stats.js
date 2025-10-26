export const financialStatsTab = {
  id: "financialStats",
  template: "tabs/financial-stats.html",
  columnCount: 11,
  dataSource: "metrics",
  render(items, ctx) {
    const { body } = ctx;
    if (!body) {
      return;
    }

    body.innerHTML = "";

    if (!items.length) {
      ctx.renderEmptyRow(body, financialStatsTab.columnCount);
      return;
    }

    items.forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${item.code}</td>
        <td>${item.name ?? ctx.emptyValue}</td>
        <td>${ctx.formatOptionalDate(item.reportingPeriod)}</td>
        <td class="${ctx.getTrendClass(item.netIncomeYoyLatest)}">${ctx.formatPercent(
        item.netIncomeYoyLatest,
        { fromRatio: true }
      )}</td>
        <td class="${ctx.getTrendClass(item.netIncomeYoyPrev1)}">${ctx.formatPercent(
        item.netIncomeYoyPrev1,
        { fromRatio: true }
      )}</td>
        <td class="${ctx.getTrendClass(item.netIncomeYoyPrev2)}">${ctx.formatPercent(
        item.netIncomeYoyPrev2,
        { fromRatio: true }
      )}</td>
        <td class="${ctx.getTrendClass(item.netIncomeQoqLatest)}">${ctx.formatPercent(
        item.netIncomeQoqLatest,
        { fromRatio: true }
      )}</td>
        <td class="${ctx.getTrendClass(item.revenueYoyLatest)}">${ctx.formatPercent(
        item.revenueYoyLatest,
        { fromRatio: true }
      )}</td>
        <td class="${ctx.getTrendClass(item.revenueQoqLatest)}">${ctx.formatPercent(
        item.revenueQoqLatest,
        { fromRatio: true }
      )}</td>
        <td class="${ctx.getTrendClass(item.roeYoyLatest)}">${ctx.formatPercent(item.roeYoyLatest, {
        fromRatio: true,
      })}</td>
        <td class="${ctx.getTrendClass(item.roeQoqLatest)}">${ctx.formatPercent(item.roeQoqLatest, {
        fromRatio: true,
      })}</td>
      `;
      body.appendChild(row);
    });
  },
};
