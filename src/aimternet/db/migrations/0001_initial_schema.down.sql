SET search_path TO ${SCHEMA};
DROP TABLE IF EXISTS member_points_ledger   CASCADE;
DROP TABLE IF EXISTS concession_order_items CASCADE;
DROP TABLE IF EXISTS concession_purchases   CASCADE;
DROP TABLE IF EXISTS rental_transactions    CASCADE;
DROP TABLE IF EXISTS members                CASCADE;
DROP TABLE IF EXISTS concession_items       CASCADE;
DROP TABLE IF EXISTS workstations           CASCADE;
