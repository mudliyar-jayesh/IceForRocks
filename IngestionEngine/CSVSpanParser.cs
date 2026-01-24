namespace IceForRocks.Ingestion;

public ref struct CSVSpanParser
{
    private ReadOnlySpan<char> _unreadChars;
    private bool _quoteSurrounded;

    public CSVSpanParser(ReadOnlySpan<char> row)
    {
        _unreadChars = row;
        _quoteSurrounded = false;
    }

    public bool HasNext() => _unreadChars.Length > 0;

    public ReadOnlySpan<char> GetNext()
    {
        if (_unreadChars.IsEmpty)
        {
            return ReadOnlySpan<char>.Empty;
        }
        /*
            One file, tortured me as I did not know most auto-generated csv files
            have quotes surrounding entries. NOW, NOW I KNOW!
        */
        bool startsWithQuote = _unreadChars.Length > 0 && _unreadChars[0] == '"';
        int delimiterIndex = -1;
        if (startsWithQuote)
        {
            // finding the next quote, for the field that is.
            int endQuote = _unreadChars.Slice(1).IndexOf('"');
            if (endQuote >= 0)
            {
                // now now, its Slice() basically gives a relative index. Found this the hard way.
                int endQuoteIndex = endQuote + 1;

                int immediateComma = _unreadChars.Slice(endQuoteIndex + 1).IndexOf(',');
                delimiterIndex = (immediateComma >= 0) ? (endQuoteIndex + immediateComma + 1) : -1;
            }
        }
        else
        {
            delimiterIndex = _unreadChars.IndexOf(',');
        }

        ReadOnlySpan<char> field;
        if (delimiterIndex == -1)
        {
            // no more commmas, end of line.
            field = _unreadChars;
            _unreadChars = ReadOnlySpan<char>.Empty;
        }
        else
        {
            field = _unreadChars.Slice(0, delimiterIndex);
            _unreadChars = _unreadChars.Slice(delimiterIndex + 1); //skip comma
        }

        // removing quotes from data
        if (startsWithQuote && field.Length >= 2 && field[field.Length - 1] == '"')
        {
            return field.Slice(1, field.Length - 2);
        }
        return field;
    }

    public unsafe void MapSpanToBytes(ReadOnlySpan<char> text, byte* buffer, int bufferSize)
    {
        if (bufferSize <= 1)
        {
            if (bufferSize > 0)
            {
                buffer[0] = 0;
            }
            return;
        }

        try
        {
            int charLimit = Math.Min(text.Length, bufferSize - 1);
            ReadOnlySpan<char> safeSlice = text.Slice(0, charLimit);

            var destination = new Span<byte>(buffer, bufferSize - 1);
            int bytesWritten = System.Text.Encoding.UTF8.GetBytes(safeSlice, destination);

            buffer[bytesWritten] = 0; // null terminate
        }
        catch (ArgumentException)
        {
            buffer[0] = 0; // ready buffer for next entry
        }
    }
}
