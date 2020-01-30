// The code below is a lambda that executes a recursive batch write to a Dynamo using the AWS Javascript SDK
// Github: ktriviso

const { marshalItem } = require('dynamodb-marshaler')
const { client } = require('*******')

// ---------------------------------------------
// lambda handler
// ---------------------------------------------
exports.handler = async (event, context) => {
    const { tableName, data } = event
    try {
        const completedBatchWrite = await batchWrite(tableName, data)
        context.succeed(completedBatchWrite)
    } catch (err) {
		context.fail(err)
	}
}

// ---------------------------------------------
// formatting for the batch write
// ---------------------------------------------
const batchWrite = async (tableName, data) => {
	await chunkArray(data, 25).map(async (group) => {
		const items = await group.map((items) => {
			const data = marshalItem(items)
			return {
				PutRequest: {
					Item: data
				}
			}
		})

		const params = {
			RequestItems: {
				[`${tableName}`]: items
			}
		}

		await batchWriteDynamoDB(params, tableName)
	})
}

// ---------------------------------------------
// dynamo recursive batch write exectution
// notes: must be recusrive to make sure there
// are no unprocessed items
// ---------------------------------------------
const batchWriteDynamoDB = async (query, tableName) => {
	await client
		.batchWriteItem(query, async (err, data) => {
			if (!err) {
				if (data.UnprocessedItems !== {}) {
					const params = {
						RequestItems: {
							[`${tableName}`]: data.UprocessedItems
						}
					}

					const result = data.UnprocessedItems[`${tableName}`]

					if (result !== undefined) await batchWriteDynamoDB(params)
				} else return
			} else return err
		})
		.promise()
}

// ---------------------------------------------
// helper function to chunk the array
// notes: aws has a lot of limitations
// ---------------------------------------------
const chunkArray = (data, chunk_size) => {
	const results = []
  
	while (data.length) {
	  results.push(local.splice(0, chunk_size))
	}
  
	return results
  }