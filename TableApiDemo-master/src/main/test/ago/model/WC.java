package ago.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class WC {
    /**
     * word
     */
    public String word;

    /**
     * 出现的次数
     */
    public long counts;
}
