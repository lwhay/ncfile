/**
 * 
 */
package trev.parallel.worker;

import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;

/**
 * @author Michael
 *
 */
public abstract class DremelsFilterScanner extends DremelsScanner {

    public abstract void config(JsonNode querRoot) throws JsonParseException, IOException;

}
