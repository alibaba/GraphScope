package ldbc.snb.datagen.test.csv;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

public class CsvFileReader implements Iterator<String[]>
{
    private static final Logger logger = Logger.getLogger( CsvFileReader.class );
    private final Pattern COLUMN_SEPARATOR_PATTERN = Pattern.compile( "\\|" );

    private final BufferedReader csvReader;

    private String[] next = null;
    private boolean closed = false;

    public CsvFileReader( File csvFile ) throws FileNotFoundException
    {
        this.csvReader = new BufferedReader( new FileReader( csvFile ) );
    }

    public boolean hasNext()
    {
        if ( closed ) return false;
        next = ( next == null ) ? nextLine() : next;
        if ( null == next ) closed = closeReader();
        return ( null != next );
    }

    public String[] next()
    {
        next = ( null == next ) ? nextLine() : next;
        if ( null == next ) throw new NoSuchElementException( "No more lines to read" );
        String[] tempNext = next;
        next = null;
        return tempNext;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private String[] nextLine()
    {
        String csvLine = null;
        try
        {
            csvLine = csvReader.readLine();
            if ( null == csvLine ) return null;
            return parseLine( csvLine );
        }
        catch ( IOException e )
        {
            String errMsg = String.format( "Error retrieving next csv entry from file [%s]", csvReader );
            logger.error( errMsg, e );
            throw new RuntimeException( errMsg, e.getCause() );
        }
    }

    private String[] parseLine( String csvLine )
    {
        return COLUMN_SEPARATOR_PATTERN.split( csvLine, -1 );
    }

    private boolean closeReader()
    {
        if ( closed )
        {
            String errMsg = "Can not close file multiple times";
            logger.error( errMsg );
            throw new RuntimeException( errMsg );
        }
        if ( csvReader == null )
        {
            String errMsg = "Can not close file - reader is null";
            logger.error( errMsg );
            throw new RuntimeException( errMsg );
        }
        try
        {
            csvReader.close();
        }
        catch ( IOException e )
        {
            String errMsg = String.format( "Error closing file [%s]", csvReader );
            logger.error( errMsg, e );
            throw new RuntimeException( errMsg, e.getCause() );
        }
        return true;
    }
}
