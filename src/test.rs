use mongodb::bson::oid::ObjectId;

#[test]
fn test_bson() {
    let d = ObjectId::new();

    dbg!(&d);

    dbg!(&d.to_string());
}