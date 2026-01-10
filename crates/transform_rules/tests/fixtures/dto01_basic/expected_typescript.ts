export interface RecordUser {
  name?: unknown;
  age: number;
}

export interface Record {
  id: string;
  user: RecordUser;
  price?: number;
  active: boolean;
  meta?: unknown;
  /** json: "user-name" */
  userName?: unknown;
  /** json: "class" */
  class_?: unknown;
  status: string;
  source: string;
}
