SELECT
  p.last_nm,
  p.frst_nm,
  (
    .689 * (bb - ibb)
    + .722 * (hbp)
    + .892 * (hits - (doubles + triples + hr))
    + 1.283 * doubles
    + 1.635 * triples
    + 2.135 * hr
  )
  /
  (
    ab + bb - ibb + sf + hbp
  ) AS woba
FROM stats s
  INNER JOIN players p ON s.player_id = p.player_id
ORDER BY woba DESC
