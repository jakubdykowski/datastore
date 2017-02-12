package qapps.datastore.local.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import qapps.datastore.local.hbase.io.KindIndex;
import qapps.datastore.local.hbase.io.SinglePropertyIndex;

enum Table {

	ENTITIES("entitiesTable", EntitiesTable.COLUMNS), BY_KIND("byKind",
			KindIndex.columns), BY_PROPERTY_ASC("byPropertyAsc",
			SinglePropertyIndex.COLUMNS), BY_PROPERTY_DESC("byPropertyDesc",
			SinglePropertyIndex.COLUMNS), ID_SEQUENCES("idSequenced",
			IdAllocator.COLUMNS), COMPOSITE("compositeProperty",
			new String[] {}) {
	};

	private final String[] columnFamilies;
	private final String name;

	private Table(String name, String[] columnFamilies) {
		this.columnFamilies = columnFamilies;
		this.name = name;
	}

	public HTable setUp(HBaseAdmin admin, String name) throws IOException {
		return setUp(admin, name, columnFamilies);
	}

	private synchronized HTable setUp(HBaseAdmin admin, String name,
			String[] cfs) throws IOException {

		if (!admin.isTableAvailable(name)) {

			HTableDescriptor table = new HTableDescriptor(name);

			for (String cf : columnFamilies) {
				table.addFamily(new HColumnDescriptor(cf));
			}
			admin.createTable(table);

		} else {

			HTableDescriptor desc = admin.getTableDescriptor(Bytes
					.toBytes(name));

			for (String cf : columnFamilies) {

				if (!desc.hasFamily(Bytes.toBytes(cf)))
					throw new IllegalStateException(
							"Datastore table schema is corrupted."
									+ " Fix it or delete corrupted tables "
									+ " and try again. Table="
									+ Bytes.toString(desc.getName()));
			}
		}
		return new HTable(admin.getConfiguration(), name);

	}

	public String getName() {
		return this.name;
	}
}
