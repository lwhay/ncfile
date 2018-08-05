/**
 * 
 */
package neci.nest.having;

import java.io.File;
import java.io.IOException;

import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.FilterOperator;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

/**
 * @author Michael
 *
 */
public class HavingTestFoo {
    public static void main(String[] args) throws IOException {
        FilterOperator[] filters = new FilterOperator[1];
        filters[0] = new FooFilterOperator();
        Schema fooSchema = new Schema.Parser().parse(new File("./src/resources/nest/foo/foomidean.avsc"));
        FilterBatchColumnReader<Record> reader =
                new FilterBatchColumnReader<>(new File("./src/resources/nest/foo/final/data0/result.neci"), filters, 1);
        reader.createSchema(fooSchema);
        reader.filter();
        reader.createFilterRead(10);
        int i = 0;
        while (reader.hasNext()) {
            Record record = reader.next();
            System.out.println(i++ + "\t" + record);
        }
        reader.close();
    }
}
