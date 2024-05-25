-- Query 1: Top 10 countries with the most gold medals
SELECT TOP 10 TeamCountry, Gold
FROM medals
ORDER BY Gold DESC;

-- Query 2: Number of athletes per discipline
SELECT a.Discipline, COUNT(*) AS AthleteCount
FROM athletes a
GROUP BY a.Discipline
ORDER BY AthleteCount DESC;

-- Query 3: Disciplines with the highest female participation
SELECT TOP 5 Discipline, Female
FROM entriesgender
ORDER BY Female DESC;

-- Query 4: Coaches and their corresponding teams
SELECT c.Name AS CoachName, t.TeamName, c.Discipline, c.Event
FROM coaches c
JOIN teams t ON c.Country = t.Country AND c.Discipline = t.Discipline
ORDER BY c.Name;

-- Query 5: Events with the highest number of medal events
SELECT a.PersonName, a.Country, COUNT(DISTINCT a.Discipline) AS DisciplineCount
FROM athletes a
JOIN medals m ON a.Country = m.TeamCountry
GROUP BY a.PersonName, a.Country
HAVING COUNT(DISTINCT a.Discipline) > 1;

-- Query 6: Medals won by each country in each discipline
SELECT m.TeamCountry, SUM(m.Gold) AS GoldMedals, SUM(m.Silver) AS SilverMedals, SUM(m.Bronze) AS BronzeMedals
FROM medals m
GROUP BY m.TeamCountry
ORDER BY GoldMedals DESC, SilverMedals DESC, BronzeMedals DESC, m.TeamCountry;

-- Query 7: Top 10 athletes with the most medals
SELECT TOP 10 a.PersonName, a.Country, a.Discipline, COUNT(*) AS TotalMedals
FROM athletes a
JOIN medals m ON a.Country = m.TeamCountry
GROUP BY a.PersonName, a.Country, a.Discipline
ORDER BY TotalMedals DESC;

-- Query 8: Countries with the most coaches
SELECT TOP 10 c.Country, COUNT(*) AS CoachCount
FROM coaches c
GROUP BY c.Country
ORDER BY CoachCount DESC;

-- Query 9: Gender distribution of athletes by country
SELECT a.Country,
       SUM(CASE WHEN e.Female > 0 THEN 1 ELSE 0 END) AS FemaleAthletes,
       SUM(CASE WHEN e.Male > 0 THEN 1 ELSE 0 END) AS MaleAthletes
FROM athletes a
JOIN entriesgender e ON a.Discipline = e.Discipline
GROUP BY a.Country;

-- Query 10: Disciplines with the highest total number of entries
SELECT TOP 10 Discipline, SUM(Total) AS TotalEntries
FROM entriesgender
GROUP BY Discipline
ORDER BY TotalEntries DESC;

-- Query 11: Countries with the most gold medals in team events
SELECT TOP 10 m.TeamCountry, COUNT(*) AS GoldMedalsTeam
FROM medals m
WHERE m.Gold = 1
GROUP BY m.TeamCountry
ORDER BY GoldMedalsTeam DESC;

-- Query 12: Athletes who have won medals in multiple disciplines
SELECT a.PersonName, a.Country, COUNT(DISTINCT a.Discipline) AS DisciplineCount
FROM athletes a
JOIN medals m ON a.Country = m.TeamCountry
GROUP BY a.PersonName, a.Country
HAVING COUNT(DISTINCT a.Discipline) > 1;

-- Query 13: Number of athletes by continent
SELECT a.Country, COUNT(*) AS AthleteCount
FROM athletes a
GROUP BY a.Country
ORDER BY AthleteCount DESC;

-- Query 14: Disciplines with the highest number of world records broken
SELECT TOP 10 t.Discipline, COUNT(*) AS MedalCount
FROM medals m
JOIN teams t ON m.TeamCountry = t.Country
GROUP BY t.Discipline
ORDER BY MedalCount DESC;

-- Query 15: Countries with the highest medal conversion rate (medals per athlete) 
SELECT TOP 10 m.TeamCountry, COUNT(DISTINCT a.PersonName) AS AthleteCount, COUNT(*) AS TotalMedals,
       COUNT(*) * 1.0 / COUNT(DISTINCT a.PersonName) AS MedalsPerAthlete
FROM medals m
JOIN athletes a ON m.TeamCountry = a.Country
GROUP BY m.TeamCountry
HAVING COUNT(DISTINCT a.PersonName) > 0
ORDER BY MedalsPerAthlete DESC;

-- Query 16: Disciplines with the highest number of participating countries
SELECT TOP 10 a.Discipline, COUNT(DISTINCT a.Country) AS ParticipatingCountries
FROM athletes a
GROUP BY a.Discipline
ORDER BY ParticipatingCountries DESC;

-- Query 17: Countries with the highest number of athletes in each discipline
SELECT a.Discipline, a.Country, COUNT(*) AS AthleteCount,
       ROW_NUMBER() OVER (PARTITION BY a.Discipline ORDER BY COUNT(*) DESC) AS Ranking
FROM athletes a
GROUP BY a.Discipline, a.Country;

-- Query 18: Coaches with the most gold medals
SELECT TOP 10 c.Name AS CoachName, c.Country, c.Discipline, COUNT(*) AS GoldMedals
FROM coaches c
JOIN medals m ON c.Country = m.TeamCountry
JOIN teams t ON c.Country = t.Country AND c.Discipline = t.Discipline
WHERE m.Gold = 1
GROUP BY c.Name, c.Country, c.Discipline
ORDER BY GoldMedals DESC;

