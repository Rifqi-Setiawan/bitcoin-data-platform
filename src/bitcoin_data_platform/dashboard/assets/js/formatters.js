/**
 * Data and value formatting utilities for Bitcoin Market Hub.
 */

/**
 * Format number as US Dollar currency string.
 * @param {number|string} val
 * @param {number} [minDecimals=2]
 * @param {number} [maxDecimals=2]
 * @returns {string}
 */
export function formatCurrency(val, minDecimals = 2, maxDecimals = 2) {
  const num = Number(val);
  if (isNaN(num)) return '$0.00';
  return '$' + num.toLocaleString('en-US', {
    minimumFractionDigits: minDecimals,
    maximumFractionDigits: maxDecimals,
  });
}

/**
 * Format number as Bitcoin precision string (8 decimals).
 * @param {number|string} val
 * @param {number} [decimals=8]
 * @returns {string}
 */
export function formatBtc(val, decimals = 8) {
  const num = Number(val);
  if (isNaN(num)) return (0).toFixed(decimals);
  return num.toFixed(decimals);
}

/**
 * Format number as percentage string.
 * @param {number|string} val
 * @param {boolean} [includeSign=true]
 * @param {number} [decimals=2]
 * @returns {string}
 */
export function formatPercent(val, includeSign = true, decimals = 2) {
  const num = Number(val);
  if (isNaN(num)) return '0.00%';
  const sign = includeSign && num > 0 ? '+' : '';
  return `${sign}${num.toFixed(decimals)}%`;
}

/**
 * Format integer or float with standard locale grouping.
 * @param {number|string} val
 * @param {number} [decimals=0]
 * @returns {string}
 */
export function formatNumber(val, decimals = 0) {
  const num = Number(val);
  if (isNaN(num)) return '0';
  return num.toLocaleString('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

/**
 * Format ISO timestamp to readable UTC date-time.
 * @param {string} isoStr
 * @returns {string}
 */
export function formatTimestamp(isoStr) {
  if (!isoStr) return '-';
  try {
    const d = new Date(isoStr);
    return d.toISOString().replace('T', ' ').substring(0, 16);
  } catch {
    return isoStr;
  }
}

/**
 * Format ISO timestamp to UTC time string.
 * @param {string} isoStr
 * @returns {string}
 */
export function formatUtcTime(isoStr) {
  if (!isoStr) return '-';
  try {
    const d = new Date(isoStr);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) + ' UTC';
  } catch {
    return isoStr;
  }
}
