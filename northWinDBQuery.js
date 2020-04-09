db.help();

// find DB's number of collection, objects, storageSize, indexes

db.stats();

//products relating to Tofu

db.products.find({ 'ProductName': /.*Tofu.*/ });

//no of products with <10 units in stock

db.products.find({ 'UnitsInStock': { $lt: 10 } }).count();

//no of products belongs to categories "Confections" with < 10 units in stock

db.products.aggregate([
    {
        $lookup: {
            from: "categories",
            localField: "CategoryID",
            foreignField: "CategoryID",
            as: "CategoryInfo"
        }
    },
    {
        $match:
            {
                'UnitsInStock': { $lt: 10 },
                'CategoryInfo.CategoryName': 'Confections'
            }
    }
]).count();

//list of category and no of products per cartegory

db.categories.aggregate([
    {
        $lookup: {
            from: "products",
            localField: "CategoryID",
            foreignField: "CategoryID",
            as: "productsInfo"
        }
    },
    {
        $project: {
            _id: 0,
            CategoryID: 1,
            CategoryName: 1,
            numberOfProducts: { $size: "$productsInfo" }
        }
    }
]);


//2nd page of 10 products sorted by product name

db.products.find().sort({ "ProductName": 1 }).skip(10).limit(10);


//top 5 customer with their product list and total spend

db.getCollection("order-details").aggregate()
    .lookup({
        from: "products",
        localField: "ProductID",
        foreignField: "ProductID",
        as: "productInfo"
    }).lookup({
        from: "orders",
        localField: "OrderID",
        foreignField: "OrderID",
        as: "orderInfo"
    }).lookup({
        from: "customers",
        localField: "orderInfo.CustomerID",
        foreignField: "CustomerID",
        as: "customerInfo"
    }).project({
        _id: 0,
        //"cusID": "$customerInfo.CustomerID",
        cusName: "$customerInfo.CompanyName",
        productList: "$productInfo.ProductName",
        totalSpend: {
            // spend after discount = (unitprice * quantity) - (unitprice * quantity)*discount 
            $subtract: [{ $multiply: ["$UnitPrice", "$Quantity"] },
            { $multiply: ["$Discount", { $multiply: ["$UnitPrice", "$Quantity"] }] }]
        }
    })
    .group({
        _id: "$cusName",
        productList: { $addToSet: "$productList" },
        totalSpend: { $sum: "$totalSpend" }
    })
    .sort({ "totalSpend": -1 }).limit(5);


//top 5 categories with top 5 products of the system


db.getCollection("order-details").aggregate([
    {
        $lookup: {
            from: "products",
            localField: "ProductID",
            foreignField: "ProductID",
            as: "productInfo"
        }
    },
    {
        $lookup: {
            from: "orders",
            localField: "OrderID",
            foreignField: "OrderID",
            as: "orderInfo"
        }
    }, {
        $lookup: {
            from: "categories",
            localField: "productInfo.CategoryID",
            foreignField: "CategoryID",
            as: "categoryInfo"
        }
    }, {
        $project: {
            _id: 0,
            CategoryName: "$categoryInfo.CategoryName",
            productList: "$productInfo.ProductName",
            totalSell: {
                // spend after discount = (unitprice * quantity) - (unitprice * quantity)*discount 
                $subtract: [{ $multiply: ["$UnitPrice", "$Quantity"] },
                { $multiply: ["$Discount", { $multiply: ["$UnitPrice", "$Quantity"] }] }]
            }
        }
    }, { $sort: { totalSell: -1 } }
    , {
        $group: {
            _id: "$CategoryName",
            productList: { $addToSet: "$productList" },
            totalSell: { $sum: "$totalSell" }
        }
    },
    {
        $project: {
            _id: "$_id",
            productList: { $slice: ["$productList", 5] },
            totalSell: -1
        }
    }

]).sort({ "totalSell": -1 }).limit(5);

// list all customer name start with "A"

db.customers.find({ ContactName: /^A/ }).explain("executionStats");

//add indexes and find again

db.customers.createIndex({ ContactName: 1 })
db.customers.find({ ContactName: /^A/ }).explain("executionStats");

//update product to embed category info inside each document:
db.products.aggregate([
    {
        $lookup: {
            from: "categories",
            localField: "CategoryID",
            foreignField: "CategoryID",
            as: "categoryInfo"
        }
    },
    //to update: merge with exact name of products, here I want to keep the products 
    //the same, so I merge into another collection
    { $merge: { into: { db: "northWindDB", coll: "productWithCategoryInfo" }, on: "_id", whenMatched: "replace", whenNotMatched: "insert" } }
]
)



//top 5 most expensive order customer with products purchased
// db.getCollection("order-details").aggregate()
//     .lookup({
//          from: "products",
//           localField: "ProductID",
//           foreignField: "ProductID",
//           as: "productInfo"
//     }).lookup({
//           from: "orders",
//           localField: "OrderID",
//           foreignField: "OrderID",
//           as: "orderInfo"
//     }).lookup({
//           from: "customers",
//           localField: "orderInfo.CustomerID",
//           foreignField: "CustomerID",
//           as: "customerInfo"
//     })  .project({  
//         _id: 0,
//         "cusID": "$customerInfo.CustomerID",
//         "cusName": "$customerInfo.CompanyName",
//         "productList": "$productInfo.ProductName",
//         totalSpend: {
//             // spend after discount = (unitprice * quantity) + (unitprice * quantity)*discount 
//             $sum: [{$multiply: [ "$UnitPrice", "$Quantity"]} , 
//                     {$multiply: ["$Discount", {$multiply: [ "$UnitPrice", "$Quantity"]}]}]}})
//         .group({
//               _id: "$cusID",
//               "totalSpend" : {$sum: "$totalSpend"}
//         })
//     .sort({"totalSpend": -1}).limit(5);
