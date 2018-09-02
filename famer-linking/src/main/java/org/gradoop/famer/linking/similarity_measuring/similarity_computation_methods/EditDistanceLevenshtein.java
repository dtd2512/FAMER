package org.gradoop.famer.linking.similarity_measuring.similarity_computation_methods;

/**
 * computes the similarity degree of two strings using the EditDistanceLevenshtein method
 */
public class EditDistanceLevenshtein {
    public double computeSimilarity(String str1, String str2){
        int m = str1.length();
        int n = str2.length();
        char[] str1Array = str1.toCharArray();
        char[] str2Array = str2.toCharArray();
        short [][] dis = new short[m+1][n+1];
        for (int i=0; i <= n; i++)
            dis[0][i]=(short) i;
        for (int i=0; i <= m; i++)
            dis[i][0]=(short) i;
        for (int i=1; i <= m; i++){
            for (int j=1; j <= n; j++){
                if (str1Array[i-1] == str2Array[j-1])
                    dis[i][j]=dis[i-1][j-1];
                else {
                    dis[i][j]= (short) Math.min(dis[i-1][j]+1,dis[i][j-1]+1);
                    dis[i][j]= (short) Math.min(dis[i][j],dis[i-1][j-1]+1);
                }
            }
        }
        double simDegree = 1-dis[m-1][n-1]/(double)Math.max(m,n);
        return simDegree;
    }
}
