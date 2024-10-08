import { Form, Item } from "@swim/structure";
import { Stock } from "./Table.types";

export class StockForm extends Form<Stock | undefined> {
  constructor() {
    super();
  }

  // Item to JS object
  override cast(item: Item): Stock | undefined {
    if (
      item.isDefinite() &&
      item.get("timestamp").isDefinite() &&
      item.get("price").isDefinite() &&
      item.get("volume").isDefinite() &&
      item.get("movement").isDefinite()
    ) {
      return {
        price: item.get("price").numberValue(0),
        volume: item.get("volume").numberValue(0),
        movement: item.get("movement").numberValue(0),
        timestamp: item.get("timestamp").numberValue(0),
        state: null,
      };
    }

    return undefined;
  }

  // JS object to Item
  override mold(object: Stock, item?: Item): Item {
    let result = Item.fromLike(object);
    if (item !== void 0) {
      result = item.concat(object);
    }
    return result;
  }
}
