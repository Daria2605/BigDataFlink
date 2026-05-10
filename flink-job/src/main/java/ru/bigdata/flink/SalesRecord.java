package ru.bigdata.flink;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class SalesRecord implements Serializable {
    private static final DateTimeFormatter US_DATE = DateTimeFormatter.ofPattern("M/d/yyyy");

    public String saleEventId;
    public String sourceFile;
    public Long sourceRowId;
    public String customerKey;
    public Long customerId;
    public String customerFirstName;
    public String customerLastName;
    public Integer customerAge;
    public String customerEmail;
    public String customerCountry;
    public String customerPostalCode;
    public String customerPetType;
    public String customerPetName;
    public String customerPetBreed;
    public String sellerKey;
    public Long sellerId;
    public String sellerFirstName;
    public String sellerLastName;
    public String sellerEmail;
    public String sellerCountry;
    public String sellerPostalCode;
    public String productKey;
    public Long productId;
    public String productName;
    public String productCategory;
    public BigDecimal productPrice;
    public Integer productQuantity;
    public Date saleDate;
    public Integer saleQuantity;
    public BigDecimal saleTotalPrice;
    public String storeKey;
    public String storeName;
    public String storeLocation;
    public String storeCity;
    public String storeState;
    public String storeCountry;
    public String storePhone;
    public String storeEmail;
    public String petCategory;
    public BigDecimal productWeight;
    public String productColor;
    public String productSize;
    public String productBrand;
    public String productMaterial;
    public String productDescription;
    public BigDecimal productRating;
    public Integer productReviews;
    public Date productReleaseDate;
    public Date productExpiryDate;
    public String supplierKey;
    public String supplierName;
    public String supplierContact;
    public String supplierEmail;
    public String supplierPhone;
    public String supplierAddress;
    public String supplierCity;
    public String supplierCountry;

    public static SalesRecord fromMap(Map<String, Object> map) {
        SalesRecord r = new SalesRecord();
        r.saleEventId = str(map, "sale_event_id");
        r.sourceFile = str(map, "source_file");
        r.sourceRowId = longValue(map, "source_row_number");
        r.customerId = longValue(map, "sale_customer_id");
        r.customerKey = key(r.sourceFile, r.customerId);
        r.customerFirstName = str(map, "customer_first_name");
        r.customerLastName = str(map, "customer_last_name");
        r.customerAge = intValue(map, "customer_age");
        r.customerEmail = str(map, "customer_email");
        r.customerCountry = str(map, "customer_country");
        r.customerPostalCode = str(map, "customer_postal_code");
        r.customerPetType = str(map, "customer_pet_type");
        r.customerPetName = str(map, "customer_pet_name");
        r.customerPetBreed = str(map, "customer_pet_breed");
        r.sellerId = longValue(map, "sale_seller_id");
        r.sellerKey = key(r.sourceFile, r.sellerId);
        r.sellerFirstName = str(map, "seller_first_name");
        r.sellerLastName = str(map, "seller_last_name");
        r.sellerEmail = str(map, "seller_email");
        r.sellerCountry = str(map, "seller_country");
        r.sellerPostalCode = str(map, "seller_postal_code");
        r.productId = longValue(map, "sale_product_id");
        r.productKey = key(r.sourceFile, r.productId);
        r.productName = str(map, "product_name");
        r.productCategory = str(map, "product_category");
        r.productPrice = decimal(map, "product_price");
        r.productQuantity = intValue(map, "product_quantity");
        r.saleDate = date(map, "sale_date");
        r.saleQuantity = intValue(map, "sale_quantity");
        r.saleTotalPrice = decimal(map, "sale_total_price");
        r.storeName = str(map, "store_name");
        r.storeKey = key(r.sourceFile, r.storeName + ":" + str(map, "store_email"));
        r.storeLocation = str(map, "store_location");
        r.storeCity = str(map, "store_city");
        r.storeState = str(map, "store_state");
        r.storeCountry = str(map, "store_country");
        r.storePhone = str(map, "store_phone");
        r.storeEmail = str(map, "store_email");
        r.petCategory = str(map, "pet_category");
        r.productWeight = decimal(map, "product_weight");
        r.productColor = str(map, "product_color");
        r.productSize = str(map, "product_size");
        r.productBrand = str(map, "product_brand");
        r.productMaterial = str(map, "product_material");
        r.productDescription = str(map, "product_description");
        r.productRating = decimal(map, "product_rating");
        r.productReviews = intValue(map, "product_reviews");
        r.productReleaseDate = date(map, "product_release_date");
        r.productExpiryDate = date(map, "product_expiry_date");
        r.supplierName = str(map, "supplier_name");
        r.supplierKey = key(r.sourceFile, r.supplierName + ":" + str(map, "supplier_email"));
        r.supplierContact = str(map, "supplier_contact");
        r.supplierEmail = str(map, "supplier_email");
        r.supplierPhone = str(map, "supplier_phone");
        r.supplierAddress = str(map, "supplier_address");
        r.supplierCity = str(map, "supplier_city");
        r.supplierCountry = str(map, "supplier_country");
        return r;
    }

    private static String key(String sourceFile, Object id) {
        return String.valueOf(sourceFile) + ":" + String.valueOf(id);
    }

    private static String str(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) return null;
        String text = value.toString().trim();
        return text.isEmpty() ? null : text;
    }

    private static Long longValue(Map<String, Object> map, String key) {
        String value = str(map, key);
        return value == null ? null : Long.parseLong(value);
    }

    private static Integer intValue(Map<String, Object> map, String key) {
        String value = str(map, key);
        return value == null ? null : Integer.parseInt(value);
    }

    private static BigDecimal decimal(Map<String, Object> map, String key) {
        String value = str(map, key);
        return value == null ? null : new BigDecimal(value);
    }

    private static Date date(Map<String, Object> map, String key) {
        String value = str(map, key);
        return value == null ? null : Date.valueOf(LocalDate.parse(value, US_DATE));
    }
}
