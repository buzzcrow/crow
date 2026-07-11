export interface FuzzySearchResult<T> {
  item: T;
  score: number;
  matches: string[];
}

export function fuzzySearch<T>(
  items: T[],
  query: string,
  getSearchFields: (item: T) => string[]
): FuzzySearchResult<T>[] {
  const lowerQuery = query.toLowerCase().trim();
  if (!lowerQuery) return items.map(item => ({ item, score: 0, matches: [] }));

  return items
    .map(item => {
      const fields = getSearchFields(item).map(f => f.toLowerCase());
      let bestScore = Infinity;
      const matches: string[] = [];

      for (const field of fields) {
        let score = 0;
        let queryIndex = 0;
        let lastMatchIndex = -1;

        for (let i = 0; i < field.length && queryIndex < lowerQuery.length; i++) {
          if (field[i] === lowerQuery[queryIndex]) {
            // Bonus for consecutive matches
            score += lastMatchIndex === i - 1 ? 1 : 2;
            // Bonus for matches at the start of the string
            score += i === 0 ? 1 : 0;
            lastMatchIndex = i;
            queryIndex++;
          } else {
            // Penalty for gaps
            score += 3;
          }
        }

        // Only include if we matched all query characters
        if (queryIndex === lowerQuery.length) {
          bestScore = Math.min(bestScore, score);
          matches.push(field);
        }
      }

      return bestScore === Infinity ? null : { item, score: bestScore, matches };
    })
    .filter((result): result is FuzzySearchResult<T> => result !== null)
    .sort((a, b) => a.score - b.score);
}
