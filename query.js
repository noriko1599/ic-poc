fromCategory("incentory")
  .foreachStream()
  .when({
    $init() {
      return {
        functionable: false,
        itemCount: 0,
        capacity: 0,
      };
    },
    ["inventory.created"](state, event) {
      state.id = event.data.id;
    },
    ["inventory.itemsAdded"](state, event) {
      state.itemCount + event.data.count;
    },
    ["inventory.itemRemoved"](state, event) {
      state.itemCount - 1;
    },
    ["inventory.capicityUpdated"](state, event) {
      state.capacity = event.data.capacity;
    },
  })
  .outputState();
