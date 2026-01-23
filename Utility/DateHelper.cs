using System.Globalization;

public static class DateHelper
{
    private static readonly string[] Formats =
    {
        "yyyyMMdd",
        "d-M-yyyy",
        "dd-MM-yyyy",
        "yyyy-MM-dd",
    };

    public static DateTime ParseSafe(string dateString)
    {
        if (string.IsNullOrWhiteSpace(dateString))
        {
            return DateTime.MinValue;
        }

        if (
            DateTime.TryParseExact(
                dateString,
                Formats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var result
            )
        )
        {
            return result;
        }

        if (DateTime.TryParse(dateString, out var backup))
        {
            return backup;
        }

        return DateTime.MinValue;
    }

    public static string ToStoreFormat(DateTime date)
    {
        return date.ToString("yyyyMMdd");
    }
}
