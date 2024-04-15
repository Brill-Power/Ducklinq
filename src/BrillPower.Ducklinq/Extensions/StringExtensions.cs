using System;

namespace BrillPower.Ducklinq.Extensions;

public static class StringExtensions
{
    public static string ToCamelCase(this string self)
    {
        // https://stackoverflow.com/questions/53180372/fastest-way-to-concatenate-readonlyspanchar-in-c-sharp
        return String.Create(self.Length, self, (neue, old) =>
        {
            neue[0] = Char.ToLower(old[0]);
            ReadOnlySpan<char> chars = old;
            chars.Slice(1).CopyTo(neue.Slice(1));
        });
    }

    public static string Pluralise(this string self)
    {
        switch (self)
        {
            case string x when x.EndsWith("um"):
                return String.Create(self.Length - 1, self, (neue, old) =>
                {
                    ReadOnlySpan<char> chars = old;
                    chars.Slice(0, chars.Length - 2).CopyTo(neue);
                    neue[neue.Length - 1] = 'a';
                });
            case string x when x.EndsWith("y"):
                return String.Create(self.Length + 2, self, (neue, old) =>
                {
                    ReadOnlySpan<char> chars = old;
                    chars.Slice(0, chars.Length - 1).CopyTo(neue);
                    ReadOnlySpan<char> ies = "ies";
                    ies.CopyTo(neue.Slice(neue.Length - 3));
                });
            default:
                return String.Create(self.Length + 1, self, (neue, old) =>
                {
                    ReadOnlySpan<char> chars = old;
                    chars.CopyTo(neue);
                    neue[neue.Length - 1] = 's';
                });
        }
    }
}
