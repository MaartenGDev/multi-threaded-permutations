using System.Collections.Generic;
using System.Linq;

namespace Aggregator
{
    public class SchemaValidator
    {
        private const int MAX_SAME_SIDE_MATCHES = 3;

        public bool IsValid(List<string> rounds)
        {
            var matchesByRound = GetMatchesByRound(rounds);

            return ValidatePlaySideForSameTeams(matchesByRound) && ValidatePlaySideForAnyTeam(matchesByRound) &&
                   ValidatePlayingAgainstSameTeam(matchesByRound, rounds[0].ToCharArray());
        }

        public List<IGrouping<int, Match>> GetMatchesByRound(List<string> rounds)
        {
            var matches = new List<Match>();

            var roundId = 0;
            int matchPlacement = 0;
            foreach (var teams in rounds.Select(round => round.ToCharArray()))
            {
                var maxTeamId = 1;
                foreach (var _ in Enumerable.Range(1, teams.Length / 2))
                {
                    matches.Add(new Match()
                    {
                        Placement = matchPlacement,
                        Round = roundId,
                        HomeTeam = teams[maxTeamId - 1],
                        AwayTeam = teams[maxTeamId],
                    });

                    maxTeamId += 2;
                    matchPlacement++;
                }

                roundId++;
            }

            return matches.GroupBy(x => x.Round).ToList();
        }

        private bool ValidatePlaySideForSameTeams(List<IGrouping<int, Match>> matchesByRound)
        {
            foreach (var matchesInRound in matchesByRound)
            {
                if (matchesInRound.Key == 0)
                {
                    continue;
                }

                foreach (var match in matchesInRound)
                {
                    for (var i = matchesInRound.Key - 1; i >= 0; i--)
                    {
                        var previousRound = matchesByRound[i];
                        var previousMatchWithSameTeams =
                            previousRound.FirstOrDefault(x => x.HasTeams(match.HomeTeam, match.AwayTeam));

                        if (previousMatchWithSameTeams == null) continue;

                        if (previousMatchWithSameTeams.HomeTeam.Equals(match.HomeTeam) &&
                            previousMatchWithSameTeams.AwayTeam.Equals(match.AwayTeam))
                        {
                            return false;
                        }

                        break;
                    }
                }
            }

            return true;
        }

        private bool ValidatePlayingAgainstSameTeam(List<IGrouping<int, Match>> matchesByRound, char[] teams)
        {
            var matchesPlayed = new Dictionary<char, Dictionary<char, int>>();
            
            foreach (var matchesInRound in matchesByRound)
            {
                foreach (var match in matchesInRound)
                {
                    if (!matchesPlayed.ContainsKey(match.HomeTeam))
                    {
                        matchesPlayed.Add(match.HomeTeam, new Dictionary<char, int>());

                        foreach (var team in teams.Where(x => !x.Equals(match.HomeTeam)))
                        {
                            matchesPlayed[match.HomeTeam].Add(team, 0);
                        }
                    }

                    if (!matchesPlayed.ContainsKey(match.AwayTeam))
                    {
                        matchesPlayed.Add(match.AwayTeam, new Dictionary<char, int>());

                        foreach (var team in teams.Where(x => !x.Equals(match.AwayTeam)))
                        {
                            matchesPlayed[match.AwayTeam].Add(team, 0);

                        }
                    }

                    matchesPlayed[match.HomeTeam][match.AwayTeam]++;
                    matchesPlayed[match.AwayTeam][match.HomeTeam]++;

                    // if the gap is bigger than one, a team has been picked before it has played against all others
                    var homePlayGap = matchesPlayed[match.HomeTeam].Values.Max() -
                                      matchesPlayed[match.HomeTeam].Values.Min();
                    var awayPlayGap = matchesPlayed[match.AwayTeam].Values.Max() -
                                      matchesPlayed[match.AwayTeam].Values.Min();
                    
                    if (homePlayGap > 1 || awayPlayGap > 1)
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        private bool ValidatePlaySideForAnyTeam(List<IGrouping<int, Match>> matchesByRound)
        {
            var homePlayedByTeam = new Dictionary<char, int>();
            var awayPlayedByTeam = new Dictionary<char, int>();

            foreach (var round in matchesByRound)
            {
                foreach (var match in round)
                {
                    if (!homePlayedByTeam.ContainsKey(match.HomeTeam))
                    {
                        homePlayedByTeam.Add(match.HomeTeam, 0);
                    }

                    if (!awayPlayedByTeam.ContainsKey(match.AwayTeam))
                    {
                        awayPlayedByTeam.Add(match.AwayTeam, 0);
                    }

                    homePlayedByTeam[match.HomeTeam]++;
                    homePlayedByTeam[match.AwayTeam] = 0;

                    awayPlayedByTeam[match.AwayTeam]++;
                    awayPlayedByTeam[match.HomeTeam] = 0;


                    if (homePlayedByTeam[match.HomeTeam] > MAX_SAME_SIDE_MATCHES ||
                        awayPlayedByTeam[match.AwayTeam] > MAX_SAME_SIDE_MATCHES)
                    {
                        return false;
                    }
                }
            }

            return true;
        }
    }
}