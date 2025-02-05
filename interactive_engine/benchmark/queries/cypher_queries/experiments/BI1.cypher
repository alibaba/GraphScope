MATCH (message:COMMENT)
WHERE message.creationDate < $datetime
WITH count(message) AS totalMessageCount

MATCH (message:COMMENT)
WHERE message.creationDate < $datetime
AND message.length > 0
WITH
  totalMessageCount,
  message,
  date(datetime({epochMillis: message.creationDate})) AS date
WITH
  totalMessageCount,
  date.year AS year,
  CASE
    WHEN 'POST' in labels(message)  THEN 0
    ELSE                                 1
    END AS isComment,
  CASE
    WHEN message.length <  40 THEN 0
    WHEN message.length <  80 THEN 1
    WHEN message.length < 160 THEN 2
    ELSE                           3
    END AS lengthCategory,
  count(message) AS messageCount,
  sum(message.length) / count(message) AS averageMessageLength,
  count(message.length) AS sumMessageLength

RETURN
  year,
  isComment,
  lengthCategory,
  messageCount,
  averageMessageLength,
  sumMessageLength,
  messageCount / totalMessageCount AS percentageOfMessages
  ORDER BY
  year DESC,
  isComment ASC,
  lengthCategory ASC;