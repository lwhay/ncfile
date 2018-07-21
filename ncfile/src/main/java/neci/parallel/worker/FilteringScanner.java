/**
 * 
 */
package neci.parallel.worker;

import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;

/**
 * @author Michael
 *
 */
public abstract class FilteringScanner extends FilterScanThread {
    public abstract void config(JsonNode querRoot) throws JsonParseException, IOException;
}
