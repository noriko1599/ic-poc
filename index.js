const client = require("./client");
const { readFileSync } = require("fs");
const { jsonEvent, END } = require("@eventstore/db-client");
const { v4: uuid } = require("uuid");

const projectionName = "inventory-ic-state";

function success(data) {
  return {
    tag: "success",
    data,
  };
}

function fail(error) {
  return {
    tag: "fail",
    error,
  };
}

async function getState(id) {
  try {
    const state = await client.getProjectionResult(projectionName, {
      fromPartition: id,
    });
    return success(state);
  } catch (error) {
    return fail({ code: error.code, message: error.message });
  }
}

async function getProjection() {
  try {
    const projection = await client.getProjectionStatistics(projectionName);
    return success(projection);
  } catch (error) {
    return fail({ code: error.code });
  }
}

async function reducer(resolvedEvent) {
  switch (resolvedEvent?.event?.type) {
    case "ic.checkIsInventoryCapacityAvailable":
      checkIsInventoryCapacityAvailableHandler(resolvedEvent);
      break;
    case "ic.checkIsInventoryFuncationable":
      checkIsInventoryFuncationableHandler(resolvedEvent);
      break;
    default:
      break;
  }
}

async function checkIsInventoryCapacityAvailableHandler(resolvedEvent) {
  const getStateResult = await getState(resolvedEvent.event.data.inventoryId);
  if (getStateResult.tag === "fail") {
    if (getStateResult.error.code === 2) {
      const exception = jsonEvent({
        type: "ic.inventoryNotFound",
        data: {
          ...getStateResult.error,
          inventoryId: resolvedEvent.event.data.inventoryId,
        },
        metadata: {
          ...resolvedEvent.event.metadata,
        },
      });

      return await client.appendToStream(`exception-ic`, [exception]);
    }
  }
  if (!getStateResult.data.hasOwnProperty("id")) {
    const exception = jsonEvent({
      type: "ic.inventoryNotFound",
      data: {
        inventoryId: resolvedEvent.event.data.inventoryId,
      },
      metadata: {
        ...resolvedEvent.event.metadata,
      },
    });

    return await client.appendToStream(`exception-ic`, [exception]);
  }

  const { itemCount } = getStateResult.data;

  const data = {
    available: itemCount + resolvedEvent.event.data.newItemCount,
  };

  const event = jsonEvent({
    type: "ic.inventoryCapacityAvailableChecked",
    data,
    metadata: {
      ...resolvedEvent.event.metadata,
    },
  });

  await client.appendToStream(`ic`, [event]);
}

async function checkIsInventoryFuncationableHandler(resolvedEvent) {
  const getStateResult = await getState(resolvedEvent.event.data.inventoryId);

  if (getStateResult.tag === "fail") {
    if (getStateResult.error.code === 2) {
      const exception = jsonEvent({
        type: "ic.inventoryNotFound",
        data: {
          ...getStateResult.error,
          inventoryId: resolvedEvent.event.data.inventoryId,
        },
        metadata: {
          ...resolvedEvent.event.metadata,
        },
      });

      return await client.appendToStream(`exception-ic`, [exception]);
    }
  }

  if (!getStateResult.data.hasOwnProperty("id")) {
    const exception = jsonEvent({
      type: "ic.inventoryNotFound",
      data: {
        inventoryId: resolvedEvent.event.data.inventoryId,
      },
      metadata: {
        ...resolvedEvent.event.metadata,
      },
    });

    return await client.appendToStream(`exception-ic`, [exception]);
  }

  const { functionable } = getStateResult.data;

  const data = {
    functionable,
  };

  const event = jsonEvent({
    type: "ic.inventoryCapacityAvailableChecked",
    data,
    metadata: {
      ...resolvedEvent.event.metadata,
    },
  });

  await client.appendToStream(`ic`, [event]);
}

async function main() {
  const query = readFileSync(__dirname + "/query.js");
  const getProjectionResult = await getProjection();
  if (getProjectionResult.tag === "fail") {
    if (getProjectionResult.error.code === 2) {
      await client.createContinuousProjection(projectionName, query.toString());
    }
  }

  const subscription = client.subscribeToStream("command-ic", {
    resolveLinkTos: true,
    fromRevision: END,
  });

  subscription.on("data", reducer);

  const command1 = jsonEvent({
    type: "ic.checkIsInventoryCapacityAvailable",
    data: {
      inventoryId: "123",
      newItemCount: 50,
    },
    metadata: {
      $correlationId: uuid(),
    },
  });

  const command2 = jsonEvent({
    type: "ic.checkIsInventoryFuncationable",
    data: {
      inventoryId: "123",
    },
    metadata: {
      $correlationId: uuid(),
    },
  });

  await client.appendToStream(`command-ic`, [command1, command2]);
}

main().catch((error) => console.error(error));
