package ru.bigdata.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SalesStarSink extends RichSinkFunction<SalesRecord> {
    private transient Connection connection;
    private transient PreparedStatement customerStmt;
    private transient PreparedStatement sellerStmt;
    private transient PreparedStatement supplierStmt;
    private transient PreparedStatement productStmt;
    private transient PreparedStatement storeStmt;
    private transient PreparedStatement factStmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        String url = System.getenv().getOrDefault("POSTGRES_URL", "jdbc:postgresql://postgres:5432/petshop");
        String user = System.getenv().getOrDefault("POSTGRES_USER", "postgres");
        String password = System.getenv().getOrDefault("POSTGRES_PASSWORD", "postgres");
        connection = DriverManager.getConnection(url, user, password);
        connection.setAutoCommit(false);

        customerStmt = connection.prepareStatement(
            "INSERT INTO star.dim_customer (customer_key, source_customer_id, first_name, last_name, age, email, country, postal_code, pet_type, pet_name, pet_breed) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (customer_key) DO UPDATE SET " +
            "source_customer_id = EXCLUDED.source_customer_id, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, age = EXCLUDED.age, " +
            "email = EXCLUDED.email, country = EXCLUDED.country, postal_code = EXCLUDED.postal_code, " +
            "pet_type = EXCLUDED.pet_type, pet_name = EXCLUDED.pet_name, pet_breed = EXCLUDED.pet_breed"
        );
        sellerStmt = connection.prepareStatement(
            "INSERT INTO star.dim_seller (seller_key, source_seller_id, first_name, last_name, email, country, postal_code) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (seller_key) DO UPDATE SET " +
            "source_seller_id = EXCLUDED.source_seller_id, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, " +
            "email = EXCLUDED.email, country = EXCLUDED.country, postal_code = EXCLUDED.postal_code"
        );
        supplierStmt = connection.prepareStatement(
            "INSERT INTO star.dim_supplier (supplier_key, supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (supplier_key) DO UPDATE SET " +
            "supplier_name = EXCLUDED.supplier_name, supplier_contact = EXCLUDED.supplier_contact, supplier_email = EXCLUDED.supplier_email, " +
            "supplier_phone = EXCLUDED.supplier_phone, supplier_address = EXCLUDED.supplier_address, supplier_city = EXCLUDED.supplier_city, " +
            "supplier_country = EXCLUDED.supplier_country"
        );
        productStmt = connection.prepareStatement(
            "INSERT INTO star.dim_product (product_key, source_product_id, product_name, product_category, pet_category, price, stock_quantity, " +
            "weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date, supplier_key, supplier_name, supplier_email) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (product_key) DO UPDATE SET " +
            "source_product_id = EXCLUDED.source_product_id, product_name = EXCLUDED.product_name, product_category = EXCLUDED.product_category, " +
            "pet_category = EXCLUDED.pet_category, price = EXCLUDED.price, stock_quantity = EXCLUDED.stock_quantity, weight = EXCLUDED.weight, " +
            "color = EXCLUDED.color, size = EXCLUDED.size, brand = EXCLUDED.brand, material = EXCLUDED.material, description = EXCLUDED.description, " +
            "rating = EXCLUDED.rating, reviews = EXCLUDED.reviews, release_date = EXCLUDED.release_date, expiry_date = EXCLUDED.expiry_date, " +
            "supplier_key = EXCLUDED.supplier_key, supplier_name = EXCLUDED.supplier_name, supplier_email = EXCLUDED.supplier_email"
        );
        storeStmt = connection.prepareStatement(
            "INSERT INTO star.dim_store (store_key, store_name, store_location, store_city, store_state, store_country, store_phone, store_email) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (store_key) DO UPDATE SET " +
            "store_name = EXCLUDED.store_name, store_location = EXCLUDED.store_location, store_city = EXCLUDED.store_city, " +
            "store_state = EXCLUDED.store_state, store_country = EXCLUDED.store_country, store_phone = EXCLUDED.store_phone, store_email = EXCLUDED.store_email"
        );
        factStmt = connection.prepareStatement(
            "INSERT INTO star.fact_sales (sale_event_id, source_file, source_row_id, sale_date, customer_key, seller_key, product_key, " +
            "store_key, supplier_key, sale_quantity, sale_total_price) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (sale_event_id) DO UPDATE SET " +
            "source_file = EXCLUDED.source_file, source_row_id = EXCLUDED.source_row_id, sale_date = EXCLUDED.sale_date, " +
            "customer_key = EXCLUDED.customer_key, seller_key = EXCLUDED.seller_key, product_key = EXCLUDED.product_key, " +
            "store_key = EXCLUDED.store_key, supplier_key = EXCLUDED.supplier_key, sale_quantity = EXCLUDED.sale_quantity, " +
            "sale_total_price = EXCLUDED.sale_total_price, ingestion_time = CURRENT_TIMESTAMP"
        );
    }

    @Override
    public void invoke(SalesRecord r, Context context) throws Exception {
        bindCustomer(r);
        bindSeller(r);
        bindSupplier(r);
        bindProduct(r);
        bindStore(r);
        bindFact(r);
        customerStmt.executeUpdate();
        sellerStmt.executeUpdate();
        supplierStmt.executeUpdate();
        productStmt.executeUpdate();
        storeStmt.executeUpdate();
        factStmt.executeUpdate();
        connection.commit();
    }

    private void bindCustomer(SalesRecord r) throws SQLException {
        customerStmt.setString(1, r.customerKey);
        customerStmt.setLong(2, r.customerId);
        customerStmt.setString(3, r.customerFirstName);
        customerStmt.setString(4, r.customerLastName);
        setInteger(customerStmt, 5, r.customerAge);
        customerStmt.setString(6, r.customerEmail);
        customerStmt.setString(7, r.customerCountry);
        customerStmt.setString(8, r.customerPostalCode);
        customerStmt.setString(9, r.customerPetType);
        customerStmt.setString(10, r.customerPetName);
        customerStmt.setString(11, r.customerPetBreed);
    }

    private void bindSeller(SalesRecord r) throws SQLException {
        sellerStmt.setString(1, r.sellerKey);
        sellerStmt.setLong(2, r.sellerId);
        sellerStmt.setString(3, r.sellerFirstName);
        sellerStmt.setString(4, r.sellerLastName);
        sellerStmt.setString(5, r.sellerEmail);
        sellerStmt.setString(6, r.sellerCountry);
        sellerStmt.setString(7, r.sellerPostalCode);
    }

    private void bindSupplier(SalesRecord r) throws SQLException {
        supplierStmt.setString(1, r.supplierKey);
        supplierStmt.setString(2, r.supplierName);
        supplierStmt.setString(3, r.supplierContact);
        supplierStmt.setString(4, r.supplierEmail);
        supplierStmt.setString(5, r.supplierPhone);
        supplierStmt.setString(6, r.supplierAddress);
        supplierStmt.setString(7, r.supplierCity);
        supplierStmt.setString(8, r.supplierCountry);
    }

    private void bindProduct(SalesRecord r) throws SQLException {
        productStmt.setString(1, r.productKey);
        productStmt.setLong(2, r.productId);
        productStmt.setString(3, r.productName);
        productStmt.setString(4, r.productCategory);
        productStmt.setString(5, r.petCategory);
        setBigDecimal(productStmt, 6, r.productPrice);
        setInteger(productStmt, 7, r.productQuantity);
        setBigDecimal(productStmt, 8, r.productWeight);
        productStmt.setString(9, r.productColor);
        productStmt.setString(10, r.productSize);
        productStmt.setString(11, r.productBrand);
        productStmt.setString(12, r.productMaterial);
        productStmt.setString(13, r.productDescription);
        setBigDecimal(productStmt, 14, r.productRating);
        setInteger(productStmt, 15, r.productReviews);
        productStmt.setDate(16, r.productReleaseDate);
        productStmt.setDate(17, r.productExpiryDate);
        productStmt.setString(18, r.supplierKey);
        productStmt.setString(19, r.supplierName);
        productStmt.setString(20, r.supplierEmail);
    }

    private void bindStore(SalesRecord r) throws SQLException {
        storeStmt.setString(1, r.storeKey);
        storeStmt.setString(2, r.storeName);
        storeStmt.setString(3, r.storeLocation);
        storeStmt.setString(4, r.storeCity);
        storeStmt.setString(5, r.storeState);
        storeStmt.setString(6, r.storeCountry);
        storeStmt.setString(7, r.storePhone);
        storeStmt.setString(8, r.storeEmail);
    }

    private void bindFact(SalesRecord r) throws SQLException {
        factStmt.setString(1, r.saleEventId);
        factStmt.setString(2, r.sourceFile);
        factStmt.setLong(3, r.sourceRowId);
        factStmt.setDate(4, r.saleDate);
        factStmt.setString(5, r.customerKey);
        factStmt.setString(6, r.sellerKey);
        factStmt.setString(7, r.productKey);
        factStmt.setString(8, r.storeKey);
        factStmt.setString(9, r.supplierKey);
        setInteger(factStmt, 10, r.saleQuantity);
        setBigDecimal(factStmt, 11, r.saleTotalPrice);
    }

    private void setInteger(PreparedStatement statement, int index, Integer value) throws SQLException {
        if (value == null) statement.setNull(index, java.sql.Types.INTEGER); else statement.setInt(index, value);
    }

    private void setBigDecimal(PreparedStatement statement, int index, BigDecimal value) throws SQLException {
        if (value == null) statement.setNull(index, java.sql.Types.NUMERIC); else statement.setBigDecimal(index, value);
    }

    @Override
    public void close() throws Exception {
        closeQuietly(factStmt);
        closeQuietly(storeStmt);
        closeQuietly(productStmt);
        closeQuietly(supplierStmt);
        closeQuietly(sellerStmt);
        closeQuietly(customerStmt);
        closeQuietly(connection);
    }

    private void closeQuietly(AutoCloseable closeable) {
        try { if (closeable != null) closeable.close(); } catch (Exception ignored) { }
    }
}
