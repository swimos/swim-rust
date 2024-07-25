export interface AppHeaderProps {
  search: string;
  setSearch: (search: string | ((prevSearch: string) => string)) => void;
}
