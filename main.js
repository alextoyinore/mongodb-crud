const {MongoClient} = require('mongodb')

function main() {
    const uri = "mongodb+srv://aore8030:test@cluster0.zdlkh9y.mongodb.net/?retryWrites=true&w=majority"

    const client = new MongoClient(uri)

    client.connect().then(result => {
        console.log('MongoDB connection successful')
    }).catch(err => {
        console.error(err)
    })

    // listDatabases(client)

    // createListing(client, {
    //     name: 'Lovely 2 bedroom',
    //     summary: 'Lovely 2 bedroom in Agege',
    //     bedrooms: 2,
    //     bathrooms: 2
    // })

    // createMultipleListings(client, [
    //     {
    //         name: "Beautiful Beach House",
    //         summary: "Enjoy relaxed beach living in this house with a private beach",
    //         bedrooms: 4,
    //         bathrooms: 4,
    //         beds: 7,
    //         last_review: new Date()
    //     },

    //     {
    //         name: "Beautiful Cottage",
    //         summary: "Enjoy a time with nature in this beautiful cottage in Ondo State, Nigeria",
    //         bedrooms: 3,
    //         bathrooms: 5,
    //         beds: 8,
    //         last_review: new Date()
    //     },

    //     {
    //         name: "Penthouse Apartment",
    //         summary: "Enjoy your weekends away from the noise in Eko Atlantic City",
    //         bedrooms: 3,
    //         bathrooms: 5,
    //         beds: 6,
    //         last_review: new Date()
    //     }
    // ])

    // findOneListingByName(client, 'Beautiful Cottage')

    // findListingWithFilter(client, {
    //     beds: {$gte: 5},
    //     bedrooms: {$gte: 3},
    //     bathrooms: {$gte: 4}
    // }, 'last_review', 2)

    // updateListingByName(client, 'Penthouse Apartment', {bedrooms: 6, bathrooms: 8})

    // upsertListingByName(client, 'Beach Cottage', {name: 'Beach Cottage', bedrooms: 6, bathrooms: 10})

    // updateAllListingsWithPropertyType(client)

    // deleteListingByName(client, 'Beautiful Cottage')

    // deleteListingScrapedBeforeDate(client, new Date("2019-02-15"))

    // AGGREGATES
    printCheapestSuburbs(client, 'Australia', 'Sydney', 5)
}

main()

// READ

function listDatabases(client) {
    client.db().admin().listDatabases()
    .then(result => {
        result.databases.forEach(element => {
            console.log(element.name)
        });
    }).catch(err => {
        console.error(err)
    })
}


function findOneListingByName(client, listingName){
    client.db('sample_airbnb')
    .collection('listingsAndReviews')
    .findOne({name: listingName})
    .then(result => {
        result != null ? console.log(`Found listing with name ${listingName}`) : console.log('No result found')
    }).catch(err => {
        console.error(err)
    })
}

function findListingWithFilter(
    client, filters={}, orderBy='name', 
    limit=Number.MAX_SAFE_INTEGER){
    client.db('sample_airbnb')
    .collection('listingsAndReviews')
    .find(filters)
    .sort({orderBy: -1})
    .limit(limit).toArray()
    .then(results => {
        results.forEach((element, i) => {
            console.log()
            console.log(`${i + 1}. name: ${element.name}`)
            console.log(`_id: ${element._id}`)
            console.log(`bedrooms: ${element.bedrooms}`)
            console.log(`bathrooms: ${element.bathrooms}`)
            console.log(`most recent review date ${new Date(element.last_review)}`)
        })
    }).catch(err => {
        console.error(err)
    })
}

// CREATE
function createOneListing(client, newListing) {
    client.db('sample_airbnb')
    .collection('listingsAndReviews')
    .insertOne(newListing)
    .then(result => {
        console.log(`New listing created with id: ${result.insertedId}`)
    }).catch(err => {
        console.error(err)
    })
}


function createMultipleListings(client, newListings) {
    client.db('sample_airbnb')
    .collection('listingsAndReviews')
    .insertMany(newListings)
    .then(result => {
        console.log(`${result.insertedCount} New listings were created with ids:`)
        console.log(result.insertedIds)
    }).catch(err => {
        console.error(err)
    })
}


// UPDATE

function updateListingByName(client, nameofListing, updatedListing){
    client.db('sample_airbnb')
    .collection('listingsAndReviews')
    .updateOne({name: nameofListing}, {$set: updatedListing})
    .then(result => {
        console.log(`${result.matchedCount} document(s) found`)
        console.log(`${result.modifiedCount} document(s) modified`)
    }).catch(err => {
        console.error(err)
    })
}

function upsertListingByName(client, nameofListing, updatedListing){
    client.db('sample_airbnb')
    .collection('listingsAndReviews')
    .updateOne({name: nameofListing}, {$set: updatedListing}, {upsert: true})
    .then(result => {
        console.log(`${result.matchedCount} document(s) found`)

        if(result.upsertedCount > 0){
            console.log(`One document was inserted with the id: ${result.upsertedId}`)
        }else{
            console.log(`${result.modifiedCount} document(s) modified`)
        }
    }).catch(err => {
        console.error(err)
    })
}


function updateAllListingsWithPropertyType(client){
    client.db('sample_airbnb')
    .collection('listingsAndReviews')
    .updateMany({property_type: {$exists: true}}, {$set: {property_type: 'Unknown'}})
    .then(results => {
        console.log(`${results.matchedCount} document(s) matched query criteria`)
        console.log(`${results.modifiedCount} document(s) modified`)
    }).catch(err => {
        console.error(err)
    })
}


function deleteListingByName(client, nameofListing){
    client.db('sample_airbnb')
    .collection('listingsAndReviews')
    .deleteOne({name: nameofListing})
    .then(result => {
        console.log(`${result.deletedCount} document(s) was deleted`)
    }).catch(err => {
        console.error(err)
    })
}


function deleteListingScrapedBeforeDate(client, date){
    client.db('sample_airbnb')
    .collection('listingsAndReviews')
    .deleteMany({last_scraped: {$lt: date}})
    .then(result => {
        console.log(`${result.deletedCount} document(s) were deleted`)
    }).catch(err => {
        console.error(err)
    })
}


// AGGREGATION

function printCheapestSuburbs(client, country, market, limit){
    const pipeline = [
        {
            '$match': {
            'bedrooms': 1, 
            'address.country': country, 
            'address.market': market, 
            'address.suburb': {
                '$exists': 1, 
                '$ne': ''
            }, 
            'room_type': 'Entire home/apt'
            }
        }, {
            '$group': {
            '_id': '$address.suburb', 
            'averagePrice': {
                '$avg': '$price'
            }
            }
        }, {
            '$sort': {
            'averagePrice': 1
            }
        }, {
            '$limit': limit
        }
    ]
    

    client.db('sample_airbnb')
    .collection('listingsAndReviews')
    .aggregate(pipeline).toArray()
    .then(results => {
        results.forEach(result => {
            console.log(`${result._id}: ${result.averagePrice}`)
        })
    }).catch(err => {
        console.log(err)
    })
}

