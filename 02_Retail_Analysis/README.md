## Retailed Analysis##

This assignment is based on ''UserBehavior.csv'' (https://tianchi.aliyun.com/dataset/649): 

This is a dataset provided by Alibaba, used for research on implicit feedback recommendation problems within the context of Taobao user behavior.

**UserBehavior.csv**

This dataset contains all behaviors of approximately one million random users between November 25, 2017, and December 3, 2017, including clicks, purchases, additions to cart, and likes. The organization of the dataset is similar to MovieLens-20M, where each row represents one user behavior, composed of user ID, item ID, item category ID, behavior type, and timestamp, separated by commas. Detailed descriptions of each column in the dataset are as follows:：

| Column Names     | Description                              |
| ---------------- | ---------------------------------------- |
| User ID          | Integer type, serialized user ID         |
| Item ID          | Integer type, serialized item ID         |
| Item Category ID | Integer type, serialized item category ID |
| Behavior Type    | String, enumerated type, including ('pv', 'buy', 'cart', 'fav') |
| Timestamp        | Timestamp indicating when the behavior occurred |

The four behavior types are as follows:

| behavior types | Description                              |
| -------------- | ---------------------------------------- |
| pv             | Page view of the item detail page, equivalent to a click |
| buy            | Purchase of the item                     |
| cart           | Adding the item to the shopping cart     |
| fav            | Favoriting the item                      |

Here are some notes about the dataset size:

| Size                      | Amount      |
| ------------------------- | ----------- |
| Number of Items           | 987,994     |
| Number of Items           | 4,162,024   |
| Number of Items           | 987,994     |
| Number of User Behaviors  | 9,439       |
| Number of Item Categories | 100,150,807 |