export const tradingDataTab = {
  id: "tradingData",
  template: "tabs/trading-data.html",
  columnCount: 12,
  dataSource: "trading",
  render(items, ctx) {
    const { body } = ctx;
    if (!body) {
      return;
    }

    body.innerHTML = "";

    if (!items.length) {
      ctx.renderEmptyRow(body, tradingDataTab.columnCount);
      return;
    }

    items.forEach((item) => {
      const marketLabel = ctx.getMarketLabel(item.market);
      const exchangeLabel = ctx.getExchangeLabel(item.exchange);
      const changeClass = ctx.getTrendClass(item.pct_change);
      const detailUrl = `stock-detail.html?code=${encodeURIComponent(item.code)}`;
      const isFavorite = Boolean(item.isFavorite || item.is_favorite);
      const favoriteLabel = ctx.favoriteLabel || "Watchlist";
      const favoriteBadge = isFavorite
        ? `<span class="favorite-badge" role="img" aria-label="${favoriteLabel}" title="${favoriteLabel}">&#10084;</span>`
        : "";
      const codeCell = `<a class="table-link" href="${detailUrl}">${item.code}</a>${favoriteBadge}`;
      const nameCell =
        item.name !== null && item.name !== undefined && item.name !== ""
          ? `<a class="table-link" href="${detailUrl}">${item.name}</a>`
          : ctx.emptyValue;
      const rawGroup = item.favoriteGroup ?? item.favorite_group ?? null;
      const groupCell =
        isFavorite || ctx.isFavoritesMode
          ? ctx.getFavoriteGroupLabel(rawGroup)
          : ctx.emptyValue;

      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${codeCell}</td>
        <td>${nameCell}</td>
        <td>${groupCell}</td>
        <td>${item.industry ?? ctx.emptyValue}</td>
        <td>${marketLabel ?? ctx.emptyValue}</td>
        <td>${exchangeLabel ?? ctx.emptyValue}</td>
        <td>${ctx.formatOptionalNumber(item.last_price, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })}</td>
        <td class="${changeClass}">${ctx.formatPercent(item.pct_change)}</td>
        <td>${ctx.formatOptionalNumber(item.volume, { maximumFractionDigits: 0 })}</td>
        <td>${ctx.formatOptionalNumber(item.market_cap, { maximumFractionDigits: 0 })}</td>
        <td>${ctx.formatOptionalNumber(item.pe_ratio, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })}</td>
        <td>${ctx.formatOptionalNumber(item.turnover_rate, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })}</td>
      `;
      body.appendChild(row);
    });
  },
};
