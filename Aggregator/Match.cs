using System.Collections.Generic;

namespace Aggregator
{
    public class Match
    {
        public int Placement { get; set; }
        public int Round { get; set; }
        public char HomeTeam { get; set; }
        public char AwayTeam { get; set; }

        public bool HasTeams(char firstTeamId, char secondTeamId)
        {
            return HomeTeam == firstTeamId && AwayTeam == secondTeamId ||
                   HomeTeam == secondTeamId && AwayTeam == firstTeamId;
        }

        public override string ToString()
        {
            return $"{HomeTeam} vs {AwayTeam}";
        }

        private sealed class RoundHomeTeamAwayTeamEqualityComparer : IEqualityComparer<Match>
        {
            public bool Equals(Match x, Match y)
            {
                if (ReferenceEquals(x, y)) return true;
                if (ReferenceEquals(x, null)) return false;
                if (ReferenceEquals(y, null)) return false;
                if (x.GetType() != y.GetType()) return false;
                return x.Round == y.Round && x.HomeTeam == y.HomeTeam && x.AwayTeam == y.AwayTeam;
            }

            public int GetHashCode(Match obj)
            {
                unchecked
                {
                    var hashCode = obj.Round;
                    hashCode = (hashCode * 397) ^ obj.HomeTeam.GetHashCode();
                    hashCode = (hashCode * 397) ^ obj.AwayTeam.GetHashCode();
                    return hashCode;
                }
            }
        }

        public static IEqualityComparer<Match> RoundHomeTeamAwayTeamComparer { get; } = new RoundHomeTeamAwayTeamEqualityComparer();
    }
    
}